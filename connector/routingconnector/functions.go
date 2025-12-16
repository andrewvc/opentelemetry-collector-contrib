// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package routingconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/routingconnector"

import (
	"context"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
)

// RouteArguments defines the arguments for the route() function.
// Pipelines can be string literals (e.g., "logs/prod") or OTTL expressions
// (e.g., Concat(["logs/", attributes["tenant"]])) that evaluate to pipeline names at runtime.
// The argument is optional - route() with no arguments is valid for backward compatibility.
type RouteArguments[K any] struct {
	Pipelines ottl.Optional[[]ottl.StringLikeGetter[K]] `ottlarg:"0"`
}

func createRouteFunction[K any](_ ottl.FunctionContext, args ottl.Arguments) (ottl.ExprFunc[K], error) {
	routeArgs, ok := args.(*RouteArguments[K])
	if !ok {
		return nil, nil
	}

	return func(ctx context.Context, tCtx K) (any, error) {
		// If no pipelines argument provided, return empty slice (legacy behavior)
		if routeArgs.Pipelines.IsEmpty() {
			return []string{}, nil
		}

		pipelineGetters := routeArgs.Pipelines.Get()
		pipelines := make([]string, 0, len(pipelineGetters))
		for _, pg := range pipelineGetters {
			p, err := pg.Get(ctx, tCtx)
			if err != nil {
				return nil, err
			}
			if p != nil {
				pipelines = append(pipelines, *p)
			}
		}
		return pipelines, nil
	}, nil
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

func spanFunctions() map[string]ottl.Factory[*ottlspan.TransformContext] {
	funcs := standardFunctions[*ottlspan.TransformContext]()

	isRootSpan := ottlfuncs.NewIsRootSpanFactoryNew()
	funcs[isRootSpan.Name()] = isRootSpan

	return funcs
}
