// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package routingconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/routingconnector"

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pipeline"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/routingconnector/internal/plogutil"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
)

type logsConnector struct {
	component.StartFunc
	component.ShutdownFunc

	logger *zap.Logger
	config *Config
	router *router[consumer.Logs]
}

func newLogsConnector(
	set connector.Settings,
	config component.Config,
	logs consumer.Logs,
) (*logsConnector, error) {
	cfg := config.(*Config)
	lr, ok := logs.(connector.LogsRouterAndConsumer)
	if !ok {
		return nil, errUnexpectedConsumer
	}

	r, err := newRouter(
		cfg.Table,
		cfg.DefaultPipelines,
		lr.Consumer,
		lr, // Pass the router for dynamic routing
		set.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	return &logsConnector{
		logger: set.Logger,
		config: cfg,
		router: r,
	}, nil
}

func (*logsConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (c *logsConnector) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	groups := make(map[consumer.Logs]plog.Logs)
	matched := plog.NewLogs()
	for i := 0; i < len(c.router.routeSlice) && ld.ResourceLogs().Len() > 0; i++ {
		var errs error
		route := c.router.routeSlice[i]

		// Determine the consumer for this route
		var routeConsumer consumer.Logs
		if route.isDynamic {
			// DYNAMIC: Evaluate route() expression to get pipeline names, then lookup consumer
			routeConsumer, errs = c.resolveDynamicConsumer(ctx, &route, ld)
			if errs != nil {
				if c.config.ErrorMode == ottl.PropagateError {
					return errs
				}
				// On error with ignore/silent mode, use default consumer
				routeConsumer = c.router.defaultConsumer
				if c.config.ErrorMode != ottl.SilentError {
					c.logger.Warn("dynamic routing failed, using default pipelines", zap.Error(errs))
				}
			}
		} else {
			// STATIC: Use pre-wired consumer (fast path)
			routeConsumer = route.consumer
		}

		switch route.statementContext {
		case "request":
			if route.requestCondition.matchRequest(ctx) {
				// all logs are routed
				ld.MoveTo(matched)
			}
		case "", "resource":
			plogutil.MoveResourcesIf(ld, matched,
				func(rl plog.ResourceLogs) bool {
					rtx := ottlresource.NewTransformContextPtr(rl.Resource(), rl)
					defer rtx.Close()
					_, isMatch, err := route.resourceStatement.Execute(ctx, rtx)
					// If error during statement evaluation consider it as not a match.
					if err != nil {
						errs = errors.Join(errs, err)
						return false
					}
					return isMatch
				},
			)
		case "log":
			plogutil.MoveRecordsWithContextIf(ld, matched,
				func(rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord) bool {
					ltx := ottllog.NewTransformContextPtr(rl, sl, lr)
					defer ltx.Close()
					_, isMatch, err := route.logStatement.Execute(ctx, ltx)
					// If error during statement evaluation consider it as not a match.
					if err != nil {
						errs = errors.Join(errs, err)
						return false
					}
					return isMatch
				},
			)
		}
		if errs != nil && c.config.ErrorMode == ottl.PropagateError {
			return errs
		}
		groupAllLogs(groups, routeConsumer, matched)
	}
	// anything left wasn't matched by any route. Send to default consumer
	groupAllLogs(groups, c.router.defaultConsumer, ld)
	var errs error
	for consumer, group := range groups {
		err := consumer.ConsumeLogs(ctx, group)
		if err != nil {
			errs = errors.Join(errs, err)
		}
	}
	return errs
}

// resolveDynamicConsumer evaluates the route() expression and looks up the consumer
func (c *logsConnector) resolveDynamicConsumer(ctx context.Context, route *routingItem[consumer.Logs], ld plog.Logs) (consumer.Logs, error) {
	// Get a sample resource to evaluate the expression
	// For resource context, we need at least one resource
	if ld.ResourceLogs().Len() == 0 {
		return nil, fmt.Errorf("no resources available for dynamic routing")
	}

	var pipelineNames []string
	var err error

	// Execute the statement to get pipeline names from route()
	switch route.statementContext {
	case "", "resource":
		rtx := ottlresource.NewTransformContextPtr(ld.ResourceLogs().At(0).Resource(), ld.ResourceLogs().At(0))
		defer rtx.Close()
		result, _, execErr := route.resourceStatement.Execute(ctx, rtx)
		if execErr != nil {
			return nil, fmt.Errorf("failed to evaluate route expression: %w", execErr)
		}
		if names, ok := result.([]string); ok {
			pipelineNames = names
		} else {
			return nil, fmt.Errorf("route() returned unexpected type %T, expected []string", result)
		}
	case "log":
		if ld.ResourceLogs().At(0).ScopeLogs().Len() == 0 || ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().Len() == 0 {
			return nil, fmt.Errorf("no log records available for dynamic routing")
		}
		ltx := ottllog.NewTransformContextPtr(
			ld.ResourceLogs().At(0),
			ld.ResourceLogs().At(0).ScopeLogs().At(0),
			ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0))
		defer ltx.Close()
		result, _, execErr := route.logStatement.Execute(ctx, ltx)
		if execErr != nil {
			return nil, fmt.Errorf("failed to evaluate route expression: %w", execErr)
		}
		if names, ok := result.([]string); ok {
			pipelineNames = names
		} else {
			return nil, fmt.Errorf("route() returned unexpected type %T, expected []string", result)
		}
	default:
		return nil, fmt.Errorf("unsupported context for dynamic routing: %s", route.statementContext)
	}

	// Convert pipeline names to IDs
	pipelineIDs := make([]pipeline.ID, len(pipelineNames))
	for i, name := range pipelineNames {
		if err = pipelineIDs[i].UnmarshalText([]byte(name)); err != nil {
			return nil, fmt.Errorf("invalid pipeline name %q: %w", name, err)
		}
	}

	// Lookup consumer from router
	if c.router.consumerRouter == nil {
		return nil, fmt.Errorf("consumer router not available for dynamic routing")
	}

	cons, err := c.router.consumerRouter.Consumer(pipelineIDs...)
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer for pipelines %v: %w", pipelineNames, err)
	}

	return cons, nil
}

func groupAllLogs(
	groups map[consumer.Logs]plog.Logs,
	cons consumer.Logs,
	logs plog.Logs,
) {
	if cons == nil {
		return
	}
	if logs.ResourceLogs().Len() == 0 {
		return
	}
	group, ok := groups[cons]
	if !ok {
		group = plog.NewLogs()
		groups[cons] = group
	}
	logs.ResourceLogs().MoveAndAppendTo(group.ResourceLogs())
}
