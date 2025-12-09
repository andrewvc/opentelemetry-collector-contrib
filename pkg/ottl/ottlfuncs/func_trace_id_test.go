// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottlfuncs

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func Test_traceID(t *testing.T) {
	tests := []struct {
		name  string
		bytes []byte
		want  pcommon.TraceID
	}{
		{
			name:  "create trace id",
			bytes: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			want:  pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exprFunc, err := traceID[any](tt.bytes)
			require.NoError(t, err)
			result, err := exprFunc(nil, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.want, result)
		})
	}
}

func Test_traceID_validation(t *testing.T) {
	tests := []struct {
		name  string
		bytes []byte
	}{
		{
			name:  "byte slice less than 16",
			bytes: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		},
		{
			name:  "byte slice longer than 16",
			bytes: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := traceID[any](tt.bytes)
			require.Error(t, err)
			assert.ErrorContains(t, err, "traces ids must be 16 bytes")
		})
	}
}

func BenchmarkTraceID(b *testing.B) {
	// Benchmark with get and set - realistic usage where we get the TraceID
	// from the expression and set it on a span
	b.Run("get_and_set", func(b *testing.B) {
		bytes := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
		expr, err := traceID[any](bytes)
		if err != nil {
			b.Fatal(err)
		}

		// Create a span to set the trace ID on
		traces := ptrace.NewTraces()
		span := traces.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()

		ctx := b.Context()
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			result, err := expr(ctx, nil)
			if err != nil {
				b.Fatal(err)
			}
			span.SetTraceID(result.(pcommon.TraceID))
		}
	})

	// Benchmark simulating dynamic trace ID setting via:
	// set(span.trace_id.string, Substring(Hex(cache["new_sha"]), 0, 32))
	// This doesn't use the TraceID function, but rather Hex + Substring + TraceID parsing
	b.Run("dynamic_hex_substring_set", func(b *testing.B) {
		// Simulate cache["new_sha"] - a 20 byte SHA1 hash
		shaBytes := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}

		// Create getters to simulate the OTTL expression
		byteGetter := &mockByteGetter{value: shaBytes}
		hexExpr, err := Hex[any](byteGetter)
		if err != nil {
			b.Fatal(err)
		}

		stringGetter := &mockStringExprGetter{expr: hexExpr}
		startGetter := &mockIntGetter{value: 0}
		lengthGetter := &mockIntGetter{value: 32}
		substringExpr := substring[any](stringGetter, startGetter, lengthGetter)

		// Create a span to set the trace ID on
		traces := ptrace.NewTraces()
		span := traces.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()

		ctx := b.Context()
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			result, err := substringExpr(ctx, nil)
			if err != nil {
				b.Fatal(err)
			}
			hexString := result.(string)
			var traceID pcommon.TraceID
			if _, err := hex.Decode(traceID[:], []byte(hexString)); err != nil {
				b.Fatal(err)
			}
			span.SetTraceID(traceID)
		}
	})
}

// Mock getters for benchmark simulation
type mockByteGetter struct {
	value []byte
}

func (m *mockByteGetter) Get(context.Context, any) ([]byte, error) {
	return m.value, nil
}

type mockStringExprGetter struct {
	expr func(context.Context, any) (any, error)
}

func (m *mockStringExprGetter) Get(ctx context.Context, tCtx any) (string, error) {
	result, err := m.expr(ctx, tCtx)
	if err != nil {
		return "", err
	}
	return result.(string), nil
}

type mockIntGetter struct {
	value int64
}

func (m *mockIntGetter) Get(context.Context, any) (int64, error) {
	return m.value, nil
}
