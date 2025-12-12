// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package routingconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/routingconnector"

import (
	"context"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
)

// RouteArguments defines the optional arguments for the route() function.
// When using string syntax like route(["pipeline1"]), the Pipelines field
// is populated by OTTL's argument parsing. The arguments are not used at
// runtime since pipelines are wired up separately during config loading.
type RouteArguments[K any] struct {
	Pipelines ottl.Optional[[]string] `ottlarg:"0"`
}

func createRouteFunction[K any](ottl.FunctionContext, ottl.Arguments) (ottl.ExprFunc[K], error) {
	return func(context.Context, K) (any, error) {
		return true, nil
	}, nil
}

// createRouteFunctionWithCapture creates a route function that captures pipelines via closure.
// This is used during router initialization to extract pipeline names from string syntax.
func createRouteFunctionWithCapture[K any](capture *[]string) func(ottl.FunctionContext, ottl.Arguments) (ottl.ExprFunc[K], error) {
	return func(_ ottl.FunctionContext, args ottl.Arguments) (ottl.ExprFunc[K], error) {
		if args != nil {
			if routeArgs, ok := args.(*RouteArguments[K]); ok {
				if !routeArgs.Pipelines.IsEmpty() {
					*capture = routeArgs.Pipelines.Get()
				}
			}
		}
		return func(context.Context, K) (any, error) {
			return true, nil
		}, nil
	}
}

func standardFunctions[K any]() map[string]ottl.Factory[K] {
	// standard converters do not transform data, so we can safely use them
	funcs := ottlfuncs.StandardConverters[K]()

	deleteKey := ottlfuncs.NewDeleteKeyFactory[K]()
	funcs[deleteKey.Name()] = deleteKey

	deleteMatchingKeys := ottlfuncs.NewDeleteMatchingKeysFactory[K]()
	funcs[deleteMatchingKeys.Name()] = deleteMatchingKeys

	route := ottl.NewFactory("route", &RouteArguments[K]{}, createRouteFunction[K])
	funcs[route.Name()] = route

	return funcs
}

// standardFunctionsWithCapture creates functions with pipeline capture for string syntax extraction.
// Used during router initialization to extract pipeline names from route(["p1", "p2"]) syntax.
func standardFunctionsWithCapture[K any](pipelineCapture *[]string) map[string]ottl.Factory[K] {
	funcs := ottlfuncs.StandardConverters[K]()

	deleteKey := ottlfuncs.NewDeleteKeyFactory[K]()
	funcs[deleteKey.Name()] = deleteKey

	deleteMatchingKeys := ottlfuncs.NewDeleteMatchingKeysFactory[K]()
	funcs[deleteMatchingKeys.Name()] = deleteMatchingKeys

	route := ottl.NewFactory("route", &RouteArguments[K]{}, createRouteFunctionWithCapture[K](pipelineCapture))
	funcs[route.Name()] = route

	return funcs
}

func spanFunctions() map[string]ottl.Factory[*ottlspan.TransformContext] {
	funcs := standardFunctions[*ottlspan.TransformContext]()

	isRootSpan := ottlfuncs.NewIsRootSpanFactoryNew()
	funcs[isRootSpan.Name()] = isRootSpan

	return funcs
}
