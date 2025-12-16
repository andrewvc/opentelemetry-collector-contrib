package routingconnector

import (
	"errors"
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/pipeline"
)

func extractRouteExprPrefix(statement string) string {
	trimmed := strings.TrimSpace(statement)
	// Extract just the route(...) part before any "where" clause.
	if whereIdx := strings.Index(trimmed, " where "); whereIdx != -1 {
		return strings.TrimSpace(trimmed[:whereIdx])
	}
	return trimmed
}

func containsOTTLFunctionCalls(routeExpr string) bool {
	// Extract the arguments inside route(...)
	start := strings.Index(routeExpr, "(")
	if start == -1 {
		return false
	}

	// Find the matching closing parenthesis
	depth := 0
	end := -1
	for i := start; i < len(routeExpr); i++ {
		if routeExpr[i] == '(' {
			depth++
		} else if routeExpr[i] == ')' {
			depth--
			if depth == 0 {
				end = i
				break
			}
		}
	}

	if end == -1 {
		return false
	}

	args := routeExpr[start+1 : end]

	// Check for common OTTL function patterns: word followed by (
	// This matches Concat(, ToLowerCase(, Split(, etc.
	for i := 0; i < len(args)-1; i++ {
		if args[i] >= 'A' && args[i] <= 'Z' || args[i] >= 'a' && args[i] <= 'z' {
			// Found start of a word, scan forward
			j := i + 1
			for j < len(args) && (args[j] >= 'A' && args[j] <= 'Z' || args[j] >= 'a' && args[j] <= 'z' || args[j] >= '0' && args[j] <= '9' || args[j] == '_') {
				j++
			}
			// Check if followed by (
			if j < len(args) && args[j] == '(' {
				return true
			}
			i = j - 1
		}
	}

	return false
}

func parsePipelineIDsFromNames(pipelineNames []string) ([]pipeline.ID, error) {
	pipelineIDs := make([]pipeline.ID, len(pipelineNames))

	var errs error
	for i, name := range pipelineNames {
		if err := pipelineIDs[i].UnmarshalText([]byte(name)); err != nil {
			errs = errors.Join(errs, fmt.Errorf("%q: %w", name, err))
		}
	}
	if errs != nil {
		return nil, errs
	}

	return pipelineIDs, nil
}
