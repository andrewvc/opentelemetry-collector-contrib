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
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pipeline"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/routingconnector/internal/pmetricutil"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottldatapoint"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlmetric"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
)

type metricsConnector struct {
	component.StartFunc
	component.ShutdownFunc

	logger *zap.Logger
	config *Config
	router *router[consumer.Metrics]
}

func newMetricsConnector(
	set connector.Settings,
	config component.Config,
	metrics consumer.Metrics,
) (*metricsConnector, error) {
	cfg := config.(*Config)
	mr, ok := metrics.(connector.MetricsRouterAndConsumer)
	if !ok {
		return nil, errUnexpectedConsumer
	}

	r, err := newRouter(
		cfg.Table,
		cfg.DefaultPipelines,
		mr.Consumer,
		mr, // Pass the router for dynamic routing
		set.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	return &metricsConnector{
		logger: set.Logger,
		config: cfg,
		router: r,
	}, nil
}

func (*metricsConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (c *metricsConnector) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	groups := make(map[consumer.Metrics]pmetric.Metrics)
	matched := pmetric.NewMetrics()
	for i := 0; i < len(c.router.routeSlice) && md.ResourceMetrics().Len() > 0; i++ {
		var errs error
		route := c.router.routeSlice[i]

		// Determine the consumer for this route
		var routeConsumer consumer.Metrics
		if route.isDynamic {
			// DYNAMIC: Evaluate route() expression to get pipeline names, then lookup consumer
			routeConsumer, errs = c.resolveDynamicConsumer(ctx, &route, md)
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
				// all metrics are routed
				md.MoveTo(matched)
			}
		case "", "resource":
			pmetricutil.MoveResourcesIf(md, matched,
				func(rs pmetric.ResourceMetrics) bool {
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
		case "metric":
			pmetricutil.MoveMetricsWithContextIf(md, matched,
				func(rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, m pmetric.Metric) bool {
					mtx := ottlmetric.NewTransformContextPtr(rm, sm, m)
					_, isMatch, err := route.metricStatement.Execute(ctx, mtx)
					mtx.Close()
					// If error during statement evaluation consider it as not a match.
					if err != nil {
						errs = errors.Join(errs, err)
						return false
					}
					return isMatch
				},
			)
		case "datapoint":
			pmetricutil.MoveDataPointsWithContextIf(md, matched,
				func(rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, m pmetric.Metric, dp any) bool {
					dptx := ottldatapoint.NewTransformContextPtr(rm, sm, m, dp)
					_, isMatch, err := route.dataPointStatement.Execute(ctx, dptx)
					dptx.Close()
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
		groupAllMetrics(groups, routeConsumer, matched)
	}
	// anything left wasn't matched by any route. Send to default consumer
	groupAllMetrics(groups, c.router.defaultConsumer, md)
	var errs error
	for consumer, group := range groups {
		err := consumer.ConsumeMetrics(ctx, group)
		if err != nil {
			errs = errors.Join(errs, err)
		}
	}
	return errs
}

// resolveDynamicConsumer evaluates the route() expression and looks up the consumer
func (c *metricsConnector) resolveDynamicConsumer(ctx context.Context, route *routingItem[consumer.Metrics], md pmetric.Metrics) (consumer.Metrics, error) {
	// Get a sample resource to evaluate the expression
	if md.ResourceMetrics().Len() == 0 {
		return nil, fmt.Errorf("no resources available for dynamic routing")
	}

	var pipelineNames []string
	var err error

	// Execute the statement to get pipeline names from route()
	switch route.statementContext {
	case "", "resource":
		rtx := ottlresource.NewTransformContextPtr(md.ResourceMetrics().At(0).Resource(), md.ResourceMetrics().At(0))
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
	case "metric":
		if md.ResourceMetrics().At(0).ScopeMetrics().Len() == 0 || md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().Len() == 0 {
			return nil, fmt.Errorf("no metrics available for dynamic routing")
		}
		mtx := ottlmetric.NewTransformContextPtr(
			md.ResourceMetrics().At(0),
			md.ResourceMetrics().At(0).ScopeMetrics().At(0),
			md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0))
		defer mtx.Close()
		result, _, execErr := route.metricStatement.Execute(ctx, mtx)
		if execErr != nil {
			return nil, fmt.Errorf("failed to evaluate route expression: %w", execErr)
		}
		if names, ok := result.([]string); ok {
			pipelineNames = names
		} else {
			return nil, fmt.Errorf("route() returned unexpected type %T, expected []string", result)
		}
	case "datapoint":
		if md.ResourceMetrics().At(0).ScopeMetrics().Len() == 0 || md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().Len() == 0 {
			return nil, fmt.Errorf("no datapoints available for dynamic routing")
		}
		m := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
		var dp any
		switch m.Type() {
		case pmetric.MetricTypeGauge:
			if m.Gauge().DataPoints().Len() > 0 {
				dp = m.Gauge().DataPoints().At(0)
			}
		case pmetric.MetricTypeSum:
			if m.Sum().DataPoints().Len() > 0 {
				dp = m.Sum().DataPoints().At(0)
			}
		case pmetric.MetricTypeHistogram:
			if m.Histogram().DataPoints().Len() > 0 {
				dp = m.Histogram().DataPoints().At(0)
			}
		case pmetric.MetricTypeExponentialHistogram:
			if m.ExponentialHistogram().DataPoints().Len() > 0 {
				dp = m.ExponentialHistogram().DataPoints().At(0)
			}
		case pmetric.MetricTypeSummary:
			if m.Summary().DataPoints().Len() > 0 {
				dp = m.Summary().DataPoints().At(0)
			}
		}
		if dp == nil {
			return nil, fmt.Errorf("no datapoints available for dynamic routing")
		}
		dptx := ottldatapoint.NewTransformContextPtr(md.ResourceMetrics().At(0), md.ResourceMetrics().At(0).ScopeMetrics().At(0), m, dp)
		defer dptx.Close()
		result, _, execErr := route.dataPointStatement.Execute(ctx, dptx)
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

func groupAllMetrics(
	groups map[consumer.Metrics]pmetric.Metrics,
	cons consumer.Metrics,
	metrics pmetric.Metrics,
) {
	if cons == nil {
		return
	}
	if metrics.ResourceMetrics().Len() == 0 {
		return
	}
	group, ok := groups[cons]
	if !ok {
		group = pmetric.NewMetrics()
		groups[cons] = group
	}
	metrics.ResourceMetrics().MoveAndAppendTo(group.ResourceMetrics())
}
