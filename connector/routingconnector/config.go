// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package routingconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/routingconnector"

import (
	"errors"
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/pipeline"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
)

var (
	errNoConditionOrStatement = errors.New("invalid route: no condition or statement provided")
	errConditionAndStatement  = errors.New("invalid route: both condition and statement provided")
	errNoPipelines            = errors.New("invalid route: no pipelines defined")
	errUnexpectedConsumer     = errors.New("expected consumer to be a connector router")
	errNoTableItems           = errors.New("invalid routing table: the routing table is empty")
)

// Config defines configuration for the Routing processor.
type Config struct {
	// ErrorMode determines how the processor reacts to errors that occur while processing an OTTL
	// condition.
	// Valid values are `ignore` and `propagate`.
	// `ignore` means the processor ignores errors returned by conditions and continues on to the
	// next condition. This is the recommended mode. If `ignore` is used and a statement's
	// condition has an error then the payload will be routed to the default exporter. `propagate`
	// means the processor returns the error up the pipeline.  This will result in the payload being
	// dropped from the collector.
	// The default value is `propagate`.
	ErrorMode ottl.ErrorMode `mapstructure:"error_mode"`
	// DefaultPipelines contains the list of pipelines to use when a more specific record can't be
	// found in the routing table.
	// Optional.
	DefaultPipelines []pipeline.ID `mapstructure:"default_pipelines"`
	// Table contains the routing table for this processor.
	// Required.
	Table []RoutingTableItem `mapstructure:"table"`
	// prevent unkeyed literal initialization
	_ struct{}
}

var _ confmap.Unmarshaler = (*Config)(nil)

// Unmarshal implements confmap.Unmarshaler to support string syntax in table entries.
//
// We cannot use the default unmarshaler (via type alias pattern) because it would fail
// when the 'table' field contains string entries. The 'Table' field is typed as
// []RoutingTableItem, so confmap would attempt to unmarshal strings into that struct type
// and fail. Instead, we manually unmarshal each field:
//   - error_mode: unmarshaled as string, then parsed into ErrorMode type
//   - default_pipelines: unmarshaled as []string, then parsed into []pipeline.ID
//   - table: custom logic to handle both string syntax and map syntax
//
// This approach is more verbose but safer and more explicit than trying to work around
// confmap's type checking.
func (cfg *Config) Unmarshal(conf *confmap.Conf) error {
	// Manually unmarshal error_mode field
	if errorMode := conf.Get("error_mode"); errorMode != nil {
		if em, ok := errorMode.(string); ok {
			if err := cfg.ErrorMode.UnmarshalText([]byte(em)); err != nil {
				return fmt.Errorf("error_mode: %w", err)
			}
		}
	}

	// Manually unmarshal default_pipelines field
	if defaultPipelines := conf.Get("default_pipelines"); defaultPipelines != nil {
		if pipelines, ok := defaultPipelines.([]any); ok {
			cfg.DefaultPipelines = make([]pipeline.ID, 0, len(pipelines))
			for i, p := range pipelines {
				if pStr, ok := p.(string); ok {
					var pipelineID pipeline.ID
					if err := pipelineID.UnmarshalText([]byte(pStr)); err != nil {
						return fmt.Errorf("default_pipelines[%d]: %w", i, err)
					}
					cfg.DefaultPipelines = append(cfg.DefaultPipelines, pipelineID)
				}
			}
		}
	}

	// Manually unmarshal table field with special handling for string syntax
	// Each entry can be either:
	//   - A string: "route(["pipeline"]) where condition" (new concise syntax)
	//   - A map: {statement: "...", pipelines: [...]} (traditional syntax)
	rawTable := conf.Get("table")
	if rawTable == nil {
		return nil
	}

	tableSlice, ok := rawTable.([]any)
	if !ok {
		return nil // Let normal validation handle this
	}

	cfg.Table = make([]RoutingTableItem, 0, len(tableSlice))
	for i, entry := range tableSlice {
		switch e := entry.(type) {
		case string:
			// String syntax: store raw, pipelines extracted at router init
			cfg.Table = append(cfg.Table, RoutingTableItem{
				Statement: e,   // Store full string: route(["pipes"]) where condition
				Pipelines: nil, // Will be populated at router init
			})
		case map[string]any:
			// Map syntax: normal unmarshaling
			var item RoutingTableItem
			itemConf := confmap.NewFromStringMap(e)
			if err := itemConf.Unmarshal(&item); err != nil {
				return fmt.Errorf("table[%d]: %w", i, err)
			}
			cfg.Table = append(cfg.Table, item)
		default:
			return fmt.Errorf("table[%d]: expected string or map, got %T", i, entry)
		}
	}

	return nil
}

// Validate checks if the processor configuration is valid.
func (c *Config) Validate() error {
	// validate that there's at least one item in the table
	if len(c.Table) == 0 {
		return errNoTableItems
	}

	// validate that every route has a value for the routing attribute and has
	// at least one pipeline
	for _, item := range c.Table {
		if item.Statement == "" && item.Condition == "" {
			return errNoConditionOrStatement
		}
		if item.Statement != "" && item.Condition != "" {
			return errConditionAndStatement
		}
		// When pipelines are empty, the configuration must still indicate how routing will happen:
		// - String syntax: `route([...]) where ...` (pipelines derived from route(...))
		// - Legacy syntax with default pipelines: `route() where ...` (pipelines omitted intentionally)
		//
		// Avoid loose substring matches: use the same prefix-based check as the router.
		if len(item.Pipelines) == 0 {
			trimmed := strings.TrimSpace(item.Statement)
			routePart := trimmed
			if whereIdx := strings.Index(trimmed, " where "); whereIdx != -1 {
				routePart = strings.TrimSpace(trimmed[:whereIdx])
			}
			if !strings.HasPrefix(routePart, "route(") {
				return errNoPipelines
			}
		}

		switch item.Context {
		case "", "resource", "span", "metric", "datapoint", "log": // ok
		case "request":
			if item.Statement != "" || item.Condition == "" {
				return fmt.Errorf("%q context requires a 'condition'", item.Context)
			}
			if _, err := parseRequestCondition(item.Condition); err != nil {
				return err
			}
		default:
			return errors.New("invalid context: " + item.Context)
		}
	}
	return nil
}

// RoutingTableItem specifies how data should be routed to the different pipelines
type RoutingTableItem struct {
	// One of "request", "resource", "log", "span", "metric", "datapoint".
	// Optional. Default "resource".
	Context string `mapstructure:"context"`

	// Statement is an OTTL statement used for making a routing decision.
	// 'Statement' is disallowed for the "request" context.
	// For other contexts, 'Statement' or 'Condition' must be provided.
	Statement string `mapstructure:"statement"`

	// Condition is an OTTL condition used for making a routing decision.
	// For the "request" context, 'Condition' is required
	// and must be of the form 'request["<attribute>"] {== | !=} <value>'.
	// For all other contexts, 'Statement' or 'Condition' must be provided, and must be a valid OTTL condition.
	Condition string `mapstructure:"condition"`

	// Pipelines contains the list of pipelines to use when the value from the FromAttribute field
	// matches this table item. When no pipelines are specified, the ones specified under
	// DefaultPipelines are used, if any.
	// The routing processor will fail upon the first failure from these pipelines.
	// Optional.
	Pipelines []pipeline.ID `mapstructure:"pipelines"`
	// prevent unkeyed literal initialization
	_ struct{}
}
