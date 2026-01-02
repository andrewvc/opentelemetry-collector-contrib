package routingconnector

import (
	"fmt"
	"strings"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottldatapoint"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlmetric"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
)

// routingItem wraps parsed OTTL statements with routing-specific metadata.
// Similar to transform processor's statement wrappers, mutations happen through methods.
type routingItem[C any] struct {
	consumer           C    // Pre-wired consumer for static routes (fast path)
	isDynamic          bool // True if route() uses expressions; false for literals
	requestCondition   *requestCondition
	resourceStatement  *ottl.Statement[*ottlresource.TransformContext]
	spanStatement      *ottl.Statement[*ottlspan.TransformContext]
	metricStatement    *ottl.Statement[*ottlmetric.TransformContext]
	dataPointStatement *ottl.Statement[*ottldatapoint.TransformContext]
	logStatement       *ottl.Statement[*ottllog.TransformContext]
	statementContext   string
}

func newRoutingItem[C any](ctx string) routingItem[C] {
	return routingItem[C]{statementContext: ctx}
}

// Dependencies for parsing statements.
type parseDeps struct {
	parserCollection *ottl.ParserCollection[any]
}

// Dependencies for classifying routes (static vs dynamic) and wiring static consumers.
type classifyDeps[C any] struct {
	tryEvaluateStatic func(routingItem[C], string) ([]string, bool)
	consumerProvider  consumerProvider[C]
}

// Dependencies for wiring legacy pipelines.
type wireDeps[C any] struct {
	consumerProvider consumerProvider[C]
}

func (ri *routingItem[C]) BuildRequest(item RoutingTableItem) error {
	reqCond, err := parseRequestCondition(item.Condition)
	if err != nil {
		return fmt.Errorf("invalid request condition for statement %q: %w", item.Statement, err)
	}
	ri.requestCondition = reqCond
	ri.isDynamic = false
	return nil
}

func (ri *routingItem[C]) ParseStatement(deps parseDeps, item RoutingTableItem) error {
	statementsGetter := ottl.NewStatementsGetter([]string{item.Statement})

	var (
		result any
		err    error
	)

	if item.Context == "" {
		result, err = deps.parserCollection.ParseStatements(statementsGetter, ottl.WithDefaultContext(ottlresource.ContextName))
	} else {
		result, err = deps.parserCollection.ParseStatementsWithContext(item.Context, statementsGetter)
	}
	if err != nil {
		return fmt.Errorf("failed to parse statement %q: %w", item.Statement, err)
	}

	switch s := result.(type) {
	case *ottl.Statement[*ottlresource.TransformContext]:
		ri.resourceStatement = s
		ri.statementContext = "resource"
	case *ottl.Statement[*ottlspan.TransformContext]:
		ri.spanStatement = s
		ri.statementContext = "span"
	case *ottl.Statement[*ottlmetric.TransformContext]:
		ri.metricStatement = s
		ri.statementContext = "metric"
	case *ottl.Statement[*ottldatapoint.TransformContext]:
		ri.dataPointStatement = s
		ri.statementContext = "datapoint"
	case *ottl.Statement[*ottllog.TransformContext]:
		ri.logStatement = s
		ri.statementContext = "log"
	default:
		return fmt.Errorf("unexpected statement type %T for statement %q", result, item.Statement)
	}

	return nil
}

func (ri *routingItem[C]) Classify(deps classifyDeps[C], item RoutingTableItem) error {
	// Legacy map syntax: always static with explicit pipelines.
	if len(item.Pipelines) > 0 {
		ri.isDynamic = false
		return nil
	}

	routeExpr := extractRouteExprPrefix(item.Statement)
	if !strings.HasPrefix(routeExpr, "route(") {
		// Defensive: config.Validate() should already guarantee this in the string-syntax path.
		return nil
	}

	// Heuristic: function calls imply dynamic.
	if containsOTTLFunctionCalls(routeExpr) {
		ri.isDynamic = true
		return nil
	}

	pipelineNames, isStatic := deps.tryEvaluateStatic(*ri, routeExpr)
	if !isStatic || len(pipelineNames) == 0 {
		ri.isDynamic = true
		return nil
	}

	pipelineIDs, err := parsePipelineIDsFromNames(pipelineNames)
	if err != nil {
		return fmt.Errorf("invalid pipeline name in statement %q: %w", item.Statement, err)
	}

	consumer, err := deps.consumerProvider(pipelineIDs...)
	if err != nil {
		return fmt.Errorf("%w for statement %q: %s", errPipelineNotFound, item.Statement, err.Error())
	}

	ri.isDynamic = false
	ri.consumer = consumer
	return nil
}

func (ri *routingItem[C]) WireLegacyPipelinesConsumer(deps wireDeps[C], item RoutingTableItem) error {
	if ri.isDynamic || len(item.Pipelines) == 0 {
		return nil
	}

	consumer, err := deps.consumerProvider(item.Pipelines...)
	if err != nil {
		return fmt.Errorf("%w for statement %q: %s", errPipelineNotFound, item.Statement, err.Error())
	}
	ri.consumer = consumer
	return nil
}
