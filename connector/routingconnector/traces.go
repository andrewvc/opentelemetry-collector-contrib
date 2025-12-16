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
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pipeline"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/routingconnector/internal/ptraceutil"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
)

type tracesConnector struct {
	component.StartFunc
	component.ShutdownFunc

	logger *zap.Logger
	config *Config
	router *router[consumer.Traces]
}

func newTracesConnector(
	set connector.Settings,
	config component.Config,
	traces consumer.Traces,
) (*tracesConnector, error) {
	cfg := config.(*Config)
	tr, ok := traces.(connector.TracesRouterAndConsumer)
	if !ok {
		return nil, errUnexpectedConsumer
	}

	r, err := newRouter(
		cfg.Table,
		cfg.DefaultPipelines,
		tr.Consumer,
		tr, // Pass the router for dynamic routing
		set.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	return &tracesConnector{
		logger: set.Logger,
		config: cfg,
		router: r,
	}, nil
}

func (*tracesConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (c *tracesConnector) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	groups := make(map[consumer.Traces]ptrace.Traces)
	matched := ptrace.NewTraces()
	for i := 0; i < len(c.router.routeSlice) && td.ResourceSpans().Len() > 0; i++ {
		var errs error
		route := c.router.routeSlice[i]

		// Determine the consumer for this route
		var routeConsumer consumer.Traces
		if route.isDynamic {
			// DYNAMIC: Evaluate route() expression to get pipeline names, then lookup consumer
			routeConsumer, errs = c.resolveDynamicConsumer(ctx, &route, td)
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
				// all traces are routed
				td.MoveTo(matched)
			}
		case "", "resource":
			ptraceutil.MoveResourcesIf(td, matched,
				func(rs ptrace.ResourceSpans) bool {
					rtx := ottlresource.NewTransformContextPtr(rs.Resource(), rs)
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
		case "span":
			ptraceutil.MoveSpansWithContextIf(td, matched,
				func(rs ptrace.ResourceSpans, ss ptrace.ScopeSpans, s ptrace.Span) bool {
					mtx := ottlspan.NewTransformContextPtr(rs, ss, s)
					defer mtx.Close()
					_, isMatch, err := route.spanStatement.Execute(ctx, mtx)
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
		groupAllTraces(groups, routeConsumer, matched)
	}
	// anything left wasn't matched by any route. Send to default consumer
	groupAllTraces(groups, c.router.defaultConsumer, td)
	var errs error
	for consumer, group := range groups {
		err := consumer.ConsumeTraces(ctx, group)
		if err != nil {
			errs = errors.Join(errs, err)
		}
	}
	return errs
}

// resolveDynamicConsumer evaluates the route() expression and looks up the consumer
func (c *tracesConnector) resolveDynamicConsumer(ctx context.Context, route *routingItem[consumer.Traces], td ptrace.Traces) (consumer.Traces, error) {
	// Get a sample resource to evaluate the expression
	if td.ResourceSpans().Len() == 0 {
		return nil, fmt.Errorf("no resources available for dynamic routing")
	}

	var pipelineNames []string
	var err error

	// Execute the statement to get pipeline names from route()
	switch route.statementContext {
	case "", "resource":
		rtx := ottlresource.NewTransformContextPtr(td.ResourceSpans().At(0).Resource(), td.ResourceSpans().At(0))
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
	case "span":
		if td.ResourceSpans().At(0).ScopeSpans().Len() == 0 || td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().Len() == 0 {
			return nil, fmt.Errorf("no spans available for dynamic routing")
		}
		mtx := ottlspan.NewTransformContextPtr(
			td.ResourceSpans().At(0),
			td.ResourceSpans().At(0).ScopeSpans().At(0),
			td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0))
		defer mtx.Close()
		result, _, execErr := route.spanStatement.Execute(ctx, mtx)
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

func groupAllTraces(
	groups map[consumer.Traces]ptrace.Traces,
	cons consumer.Traces,
	traces ptrace.Traces,
) {
	if cons == nil {
		return
	}
	if traces.ResourceSpans().Len() == 0 {
		return
	}
	group, ok := groups[cons]
	if !ok {
		group = ptrace.NewTraces()
		groups[cons] = group
	}
	traces.ResourceSpans().MoveAndAppendTo(group.ResourceSpans())
}
