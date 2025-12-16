// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package routingconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/routingconnector"

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pipeline"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottldatapoint"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlmetric"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
)

var (
	errPipelineNotFound       = errors.New("pipeline not found")
	errStatementCountMismatch = errors.New("expected exactly one statement")
)

// consumerProvider is a function with a type parameter C (expected to be one
// of consumer.Traces, consumer.Metrics, or Consumer.Logs). returns a
// consumer for the given component ID(s).
type consumerProvider[C any] func(...pipeline.ID) (C, error)

// consumerRouter is a minimal interface for dynamic routing lookups.
// It is satisfied by:
// - connector.LogsRouterAndConsumer
// - connector.TracesRouterAndConsumer
// - connector.MetricsRouterAndConsumer
//
// This avoids storing the router as `any` and avoids per-signal type assertions.
type consumerRouter[C any] interface {
	Consumer(...pipeline.ID) (C, error)
}

// router registers consumers and default consumers for a pipeline. the type
// parameter C is expected to be one of: consumer.Traces, consumer.Metrics, or
// consumer.Logs.
type router[C any] struct {
	parserCollection *ottl.ParserCollection[any]
	defaultConsumer  C
	logger           *zap.Logger
	settings         component.TelemetrySettings
	routes           map[string]routingItem[C]
	consumerProvider consumerProvider[C]
	consumerRouter   consumerRouter[C]
	table            []RoutingTableItem
	routeSlice       []routingItem[C]
}

// newRouter creates a new router instance with based on type parameters C and K.
// see router struct definition for the allowed types.
func newRouter[C any](
	table []RoutingTableItem,
	defaultPipelineIDs []pipeline.ID,
	provider consumerProvider[C],
	consumerRouter consumerRouter[C],
	settings component.TelemetrySettings,
) (*router[C], error) {
	r := &router[C]{
		logger:           settings.Logger,
		settings:         settings,
		table:            table,
		routes:           make(map[string]routingItem[C]),
		consumerProvider: provider,
		consumerRouter:   consumerRouter,
	}

	if err := r.buildParsers(table, settings); err != nil {
		return nil, err
	}

	if err := r.registerConsumers(defaultPipelineIDs); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *router[C]) buildParsers(_ []RoutingTableItem, settings component.TelemetrySettings) error {
	// Context inference priority list: when a condition uses an ambiguous path (one that exists
	// in multiple contexts), the inferrer tries each context in order until one can parse it.
	//
	// "resource" is first for backward compatibility: before context inference existed, the
	// routing connector only supported resource context. Existing configs with conditions like
	// `attributes["env"] == "prod"` must continue to resolve to resource.attributes.
	//
	// The remaining order matters less in practice because most ambiguous paths (like "attributes")
	// exist in resource anyway, and non-ambiguous paths (like "body", "severity_text", "name")
	// only exist in one context regardless of priority. That said, the order is sorted based on which
	// events are most common in practice, hence 'span' is first.
	priorities := []string{
		"resource",
		"span",
		"spanevent",
		"metric",
		"datapoint",
		"log",
		"scope",
		"instrumentation_scope",
	}

	// Create all parsers upfront. This follows the pattern used by other OTTL-using components
	// like the transform processor. The OTTL context inferrer needs access to all context
	// parsers to properly determine which context to use based on paths, functions, and enums.
	// The one-time initialization cost is minimal compared to the complexity and fragility
	// of trying to pre-determine which contexts are needed via statement inspection.
	//
	// Create parsers for the supported contexts and register them with a ParserCollection.
	resourceParser, err := ottlresource.NewParser(
		standardFunctions[*ottlresource.TransformContext](),
		settings,
		ottlresource.EnablePathContextNames(),
	)
	if err != nil {
		return err
	}
	spanParser, err := ottlspan.NewParser(
		spanFunctions(),
		settings,
		ottlspan.EnablePathContextNames(),
	)
	if err != nil {
		return err
	}
	metricParser, err := ottlmetric.NewParser(
		standardFunctions[*ottlmetric.TransformContext](),
		settings,
		ottlmetric.EnablePathContextNames(),
	)
	if err != nil {
		return err
	}
	dataPointParser, err := ottldatapoint.NewParser(
		standardFunctions[*ottldatapoint.TransformContext](),
		settings,
		ottldatapoint.EnablePathContextNames(),
	)
	if err != nil {
		return err
	}
	logParser, err := ottllog.NewParser(
		standardFunctions[*ottllog.TransformContext](),
		settings,
		ottllog.EnablePathContextNames(),
	)
	if err != nil {
		return err
	}

	r.parserCollection, err = ottl.NewParserCollection(
		settings,
		ottl.WithContextInferrerPriorities[any](priorities),
		ottl.WithParserCollectionContext(
			ottlresource.ContextName,
			&resourceParser,
			ottl.WithStatementConverter(singleStatementConverter[*ottlresource.TransformContext]()),
		),
		ottl.WithParserCollectionContext(
			ottlspan.ContextName,
			&spanParser,
			ottl.WithStatementConverter(singleStatementConverter[*ottlspan.TransformContext]()),
		),
		ottl.WithParserCollectionContext(
			ottlmetric.ContextName,
			&metricParser,
			ottl.WithStatementConverter(singleStatementConverter[*ottlmetric.TransformContext]()),
		),
		ottl.WithParserCollectionContext(
			ottldatapoint.ContextName,
			&dataPointParser,
			ottl.WithStatementConverter(singleStatementConverter[*ottldatapoint.TransformContext]()),
		),
		ottl.WithParserCollectionContext(
			ottllog.ContextName,
			&logParser,
			ottl.WithStatementConverter(singleStatementConverter[*ottllog.TransformContext]()),
		),
	)
	return err
}

// singleStatementConverter extracts a single parsed statement from the parser output.
// Unlike the transform processor which works with statement sequences, the routing connector
// evaluates one statement per route to determine where data should be routed.
//
// The length check is technically redundant since registerRouteConsumers always passes exactly
// one statement to the parser, and the OTTL parser produces one parsed statement per input.
// However, it serves as defense-in-depth against future bugs in either this code or the OTTL library.
func singleStatementConverter[K any]() ottl.ParsedStatementsConverter[K, any] {
	return func(_ *ottl.ParserCollection[any], _ ottl.StatementsGetter, parsedStatements []*ottl.Statement[K]) (any, error) {
		if len(parsedStatements) != 1 {
			return nil, fmt.Errorf("%w: got %d", errStatementCountMismatch, len(parsedStatements))
		}
		return parsedStatements[0], nil
	}
}

func (r *router[C]) registerConsumers(defaultPipelineIDs []pipeline.ID) error {
	// register default pipelines
	err := r.registerDefaultConsumer(defaultPipelineIDs)
	if err != nil {
		return err
	}

	r.normalizeConditions()

	// register pipelines for each route
	err = r.registerRouteConsumers()
	if err != nil {
		return err
	}

	return nil
}

// registerDefaultConsumer registers a consumer for the default pipelines configured
func (r *router[C]) registerDefaultConsumer(pipelineIDs []pipeline.ID) error {
	if len(pipelineIDs) == 0 {
		return nil
	}

	consumer, err := r.consumerProvider(pipelineIDs...)
	if err != nil {
		return fmt.Errorf("%w: %s", errPipelineNotFound, err.Error())
	}

	r.defaultConsumer = consumer

	return nil
}

// convert conditions to statements
func (r *router[C]) normalizeConditions() {
	for i := range r.table {
		item := &r.table[i]
		if item.Condition != "" {
			item.Statement = fmt.Sprintf("route() where %s", item.Condition)
		}
	}
}

// registerRouteConsumers registers consumers for routes, detecting static vs dynamic routing
func (r *router[C]) registerRouteConsumers() error {
	// Accumulate errors following transform processor's pattern (processor.go:31-42)
	var errs error

	classify := classifyDeps[C]{
		tryEvaluateStatic: r.tryEvaluateStaticRoute,
		consumerProvider:  r.consumerProvider,
	}
	parse := parseDeps{parserCollection: r.parserCollection}
	wire := wireDeps[C]{consumerProvider: r.consumerProvider}

	for _, item := range r.table {
		if r.checkDuplicateRoute(item) {
			continue
		}

		route := newRoutingItem[C](item.Context)

		var err error
		if item.Context == "request" {
			err = route.BuildRequest(item)
		} else {
			if err = route.ParseStatement(parse, item); err == nil {
				err = route.Classify(classify, item)
			}
		}
		if err != nil {
			errs = errors.Join(errs, err)
			continue
		}

		if err = route.WireLegacyPipelinesConsumer(wire, item); err != nil {
			errs = errors.Join(errs, err)
			continue
		}

		r.addRoute(item, route)
	}
	return errs
}

// checkDuplicateRoute checks if a route already exists in the
// routing table and logs a warning if it does.
func (r *router[C]) checkDuplicateRoute(item RoutingTableItem) bool {
	if _, ok := r.routes[key(item)]; !ok {
		return false
	}

	var pipelineNames []string
	for _, pipeline := range item.Pipelines {
		pipelineNames = append(pipelineNames, pipeline.String())
	}
	exporters := strings.Join(pipelineNames, ", ")

	r.logger.Warn(fmt.Sprintf(
		`Statement %q already exists in the routing table, the route with target pipeline(s) %q will be ignored.`,
		item.Statement,
		exporters,
	))
	return true
}

func (r *router[C]) addRoute(item RoutingTableItem, route routingItem[C]) {
	r.routeSlice = append(r.routeSlice, route)
	r.routes[key(item)] = route
}

func key(entry RoutingTableItem) string {
	switch entry.Context {
	case "", "resource":
		return entry.Statement
	case "request":
		return "[request] " + entry.Condition
	default:
		return "[" + entry.Context + "] " + entry.Statement
	}
}

// tryEvaluateStaticRoute attempts to evaluate a route expression with an empty context.
// This follows the same pattern as OTTL's newListGetter
// - If all route() arguments are literals, they return values with empty context
// - If any argument is an expression, evaluation errors or returns empty
//
// The key insight from literal_getter.go:24 is that literal[K,T].Get() ignores context:
//
//	func (l *literal[K, T]) Get(context.Context, K) (T, error) { return l.value, nil }
//
// Parsers are reused from router.buildParsers following transform processor's pattern
// to avoid creating new parsers for each route during initialization.
func (r *router[C]) tryEvaluateStaticRoute(route routingItem[C], routeExpr string) ([]string, bool) {
	ctx := context.Background()

	// Parse the route() expression using the already-initialized ParserCollection, using an explicit context.
	// Using ParseStatements (inference) here is unreliable because `route(...)` might not reference any paths.
	statementsGetter := ottl.NewStatementsGetter([]string{routeExpr})

	contextName := route.statementContext
	if contextName == "" {
		contextName = ottlresource.ContextName
	}

	parsed, err := r.parserCollection.ParseStatementsWithContext(contextName, statementsGetter)
	if err != nil {
		return nil, false
	}

	switch stmt := parsed.(type) {
	case *ottl.Statement[*ottlresource.TransformContext]:
		emptyResourceLogs := plog.NewResourceLogs()
		tCtx := ottlresource.NewTransformContextPtr(emptyResourceLogs.Resource(), emptyResourceLogs)
		defer tCtx.Close()

		result, _, err := stmt.Execute(ctx, tCtx)
		if err != nil {
			return nil, false
		}
		if pipelineNames, ok := result.([]string); ok && len(pipelineNames) > 0 {
			return pipelineNames, true
		}
		return nil, false

	case *ottl.Statement[*ottllog.TransformContext]:
		emptyResourceLogs := plog.NewResourceLogs()
		emptyScopeLogs := emptyResourceLogs.ScopeLogs().AppendEmpty()
		emptyLogRecord := emptyScopeLogs.LogRecords().AppendEmpty()
		tCtx := ottllog.NewTransformContextPtr(emptyResourceLogs, emptyScopeLogs, emptyLogRecord)
		defer tCtx.Close()

		result, _, err := stmt.Execute(ctx, tCtx)
		if err != nil {
			return nil, false
		}
		if pipelineNames, ok := result.([]string); ok && len(pipelineNames) > 0 {
			return pipelineNames, true
		}
		return nil, false

	case *ottl.Statement[*ottlspan.TransformContext]:
		emptyResourceSpans := ptrace.NewResourceSpans()
		emptyScopeSpans := emptyResourceSpans.ScopeSpans().AppendEmpty()
		emptySpan := emptyScopeSpans.Spans().AppendEmpty()
		tCtx := ottlspan.NewTransformContextPtr(emptyResourceSpans, emptyScopeSpans, emptySpan)
		defer tCtx.Close()

		result, _, err := stmt.Execute(ctx, tCtx)
		if err != nil {
			return nil, false
		}
		if pipelineNames, ok := result.([]string); ok && len(pipelineNames) > 0 {
			return pipelineNames, true
		}
		return nil, false

	case *ottl.Statement[*ottlmetric.TransformContext]:
		emptyResourceMetrics := pmetric.NewResourceMetrics()
		emptyScopeMetrics := emptyResourceMetrics.ScopeMetrics().AppendEmpty()
		emptyMetric := emptyScopeMetrics.Metrics().AppendEmpty()
		tCtx := ottlmetric.NewTransformContextPtr(emptyResourceMetrics, emptyScopeMetrics, emptyMetric)
		defer tCtx.Close()

		result, _, err := stmt.Execute(ctx, tCtx)
		if err != nil {
			return nil, false
		}
		if pipelineNames, ok := result.([]string); ok && len(pipelineNames) > 0 {
			return pipelineNames, true
		}
		return nil, false

	case *ottl.Statement[*ottldatapoint.TransformContext]:
		emptyResourceMetrics := pmetric.NewResourceMetrics()
		emptyScopeMetrics := emptyResourceMetrics.ScopeMetrics().AppendEmpty()
		emptyMetric := emptyScopeMetrics.Metrics().AppendEmpty()
		emptyGauge := emptyMetric.SetEmptyGauge()
		emptyDataPoint := emptyGauge.DataPoints().AppendEmpty()
		tCtx := ottldatapoint.NewTransformContextPtr(emptyResourceMetrics, emptyScopeMetrics, emptyMetric, emptyDataPoint)
		defer tCtx.Close()

		result, _, err := stmt.Execute(ctx, tCtx)
		if err != nil {
			return nil, false
		}
		if pipelineNames, ok := result.([]string); ok && len(pipelineNames) > 0 {
			return pipelineNames, true
		}
		return nil, false

	default:
		// Should never happen because ParserCollection is configured with a singleStatementConverter
		// and only these contexts are registered, but be defensive.
		return nil, false
	}
}
