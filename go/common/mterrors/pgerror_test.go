// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mterrors

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

func TestNewPgError(t *testing.T) {
	diag := &sqltypes.PgDiagnostic{
		MessageType: protocol.MsgErrorResponse,
		Severity:    "ERROR",
		Code:        "42P01",
		Message:     "relation \"foo\" does not exist",
		Detail:      "Table not found",
		Hint:        "Check the table name",
		Position:    15,
		Schema:      "public",
		Table:       "foo",
	}

	pgErr := NewPgError(diag)
	require.NotNil(t, pgErr)

	// Verify Error() returns clean format
	assert.Equal(t, "ERROR: relation \"foo\" does not exist", pgErr.Error())

	// Verify Diagnostic() returns all fields
	result := pgErr.Diagnostic()
	require.NotNil(t, result)
	assert.Equal(t, byte(protocol.MsgErrorResponse), result.MessageType)
	assert.Equal(t, "ERROR", result.Severity)
	assert.Equal(t, "42P01", result.Code)
	assert.Equal(t, "relation \"foo\" does not exist", result.Message)
	assert.Equal(t, "Table not found", result.Detail)
	assert.Equal(t, "Check the table name", result.Hint)
	assert.Equal(t, int32(15), result.Position)
	assert.Equal(t, "public", result.Schema)
	assert.Equal(t, "foo", result.Table)
}

func TestNewPgErrorFromDiagnostic(t *testing.T) {
	diag := &sqltypes.PgDiagnostic{
		MessageType: protocol.MsgErrorResponse,
		Severity:    "FATAL",
		Code:        "28P01",
		Message:     "password authentication failed",
		Detail:      "",
		Hint:        "",
	}

	pgErr := NewPgErrorFromDiagnostic(diag)
	require.NotNil(t, pgErr)

	assert.Equal(t, "FATAL: password authentication failed", pgErr.Error())
	assert.Equal(t, diag, pgErr.Diagnostic())
}

func TestPgErrorErrorCode(t *testing.T) {
	pgErr := NewPgErrorFromDiagnostic(&sqltypes.PgDiagnostic{
		MessageType: protocol.MsgErrorResponse,
		Severity:    "ERROR",
		Code:        "22P02",
		Message:     "invalid input syntax",
	})

	// PgError returns UNKNOWN since PostgreSQL errors don't map to gRPC codes
	assert.Equal(t, mtrpcpb.Code_UNKNOWN, pgErr.ErrorCode())
}

func TestPgErrorWithNilDiagnostic(t *testing.T) {
	pgErr := &PgError{diag: nil}
	assert.Equal(t, "ERROR: unknown error", pgErr.Error())
	assert.Equal(t, "ERROR: unknown error (SQLSTATE 00000)", pgErr.FullError())
	assert.Nil(t, pgErr.Diagnostic())
}

func TestPgErrorNilReceiver(t *testing.T) {
	// Test nil receiver safety for all PgError methods
	var pgErr *PgError

	// Error() should not panic on nil receiver
	assert.Equal(t, "ERROR: unknown error", pgErr.Error())

	// FullError() should not panic on nil receiver
	assert.Equal(t, "ERROR: unknown error (SQLSTATE 00000)", pgErr.FullError())

	// Diagnostic() should return nil on nil receiver
	assert.Nil(t, pgErr.Diagnostic())

	// ErrorCode() should not panic on nil receiver
	assert.Equal(t, mtrpcpb.Code_UNKNOWN, pgErr.ErrorCode())
}

func TestPgErrorFullError(t *testing.T) {
	pgErr := NewPgErrorFromDiagnostic(&sqltypes.PgDiagnostic{
		MessageType: protocol.MsgErrorResponse,
		Severity:    "ERROR",
		Code:        "42P01",
		Message:     "relation \"foo\" does not exist",
	})

	// Error() returns PostgreSQL-native format
	assert.Equal(t, "ERROR: relation \"foo\" does not exist", pgErr.Error())

	// FullError() includes SQLSTATE for debugging
	assert.Equal(t, "ERROR: relation \"foo\" does not exist (SQLSTATE 42P01)", pgErr.FullError())
}

func TestIsPgError(t *testing.T) {
	pgErr := NewPgErrorFromDiagnostic(&sqltypes.PgDiagnostic{
		MessageType: protocol.MsgErrorResponse,
		Severity:    "ERROR",
		Code:        "42000",
		Message:     "test error",
	})

	// Direct PgError
	assert.True(t, IsPgError(pgErr))

	// Wrapped PgError - should still be detectable
	wrappedErr := fmt.Errorf("wrapped: %w", pgErr)
	assert.True(t, IsPgError(wrappedErr))

	// Non-PgError
	assert.False(t, IsPgError(errors.New("not a pg error")))
	assert.False(t, IsPgError(nil))
}

func TestAsPgError(t *testing.T) {
	pgErr := NewPgErrorFromDiagnostic(&sqltypes.PgDiagnostic{
		MessageType: protocol.MsgErrorResponse,
		Severity:    "ERROR",
		Code:        "42000",
		Message:     "test error",
	})

	// Direct PgError
	extracted, ok := AsPgError(pgErr)
	assert.True(t, ok)
	assert.Equal(t, pgErr, extracted)

	// Wrapped PgError
	wrappedErr := fmt.Errorf("wrapped: %w", pgErr)
	extracted, ok = AsPgError(wrappedErr)
	assert.True(t, ok)
	assert.Equal(t, pgErr, extracted)

	// Non-PgError
	extracted, ok = AsPgError(errors.New("not a pg error"))
	assert.False(t, ok)
	assert.Nil(t, extracted)

	// nil error
	extracted, ok = AsPgError(nil)
	assert.False(t, ok)
	assert.Nil(t, extracted)
}

func TestPgErrorImplementsErrorWithCode(t *testing.T) {
	pgErr := NewPgErrorFromDiagnostic(&sqltypes.PgDiagnostic{
		MessageType: protocol.MsgErrorResponse,
		Severity:    "ERROR",
		Code:        "42000",
		Message:     "test error",
	})

	// Verify PgError implements ErrorWithCode
	var _ ErrorWithCode = pgErr

	// Code() function should work with PgError
	assert.Equal(t, mtrpcpb.Code_UNKNOWN, Code(pgErr))
}

func TestPgErrorUnwrap(t *testing.T) {
	pgErr := NewPgErrorFromDiagnostic(&sqltypes.PgDiagnostic{
		MessageType: protocol.MsgErrorResponse,
		Severity:    "ERROR",
		Code:        "42P01",
		Message:     "relation does not exist",
	})

	// PgError is a leaf error - Unwrap should return nil
	assert.Nil(t, pgErr.Unwrap(), "PgError.Unwrap() should return nil (leaf error)")

	// errors.As should work with direct PgError
	var extracted *PgError
	assert.True(t, errors.As(pgErr, &extracted))
	assert.Equal(t, pgErr, extracted)

	// errors.As should work with wrapped PgError
	wrappedErr := fmt.Errorf("context: %w", pgErr)
	extracted = nil
	assert.True(t, errors.As(wrappedErr, &extracted))
	assert.Equal(t, pgErr, extracted)

	// errors.As should work with deeply wrapped PgError
	deeplyWrapped := fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", pgErr))
	extracted = nil
	assert.True(t, errors.As(deeplyWrapped, &extracted))
	assert.Equal(t, pgErr, extracted)
}

func TestPgErrorErrorsIs(t *testing.T) {
	pgErr1 := NewPgErrorFromDiagnostic(&sqltypes.PgDiagnostic{
		MessageType: protocol.MsgErrorResponse,
		Severity:    "ERROR",
		Code:        "42P01",
		Message:     "relation does not exist",
	})

	pgErr2 := NewPgErrorFromDiagnostic(&sqltypes.PgDiagnostic{
		MessageType: protocol.MsgErrorResponse,
		Severity:    "ERROR",
		Code:        "42P01",
		Message:     "relation does not exist",
	})

	// errors.Is compares identity, not equality
	// Same pointer should match
	assert.True(t, errors.Is(pgErr1, pgErr1))

	// Different pointers (even with same content) should not match
	// because PgError doesn't implement Is() method
	assert.False(t, errors.Is(pgErr1, pgErr2))

	// Wrapped error should find the original via errors.Is
	wrappedErr := fmt.Errorf("wrapped: %w", pgErr1)
	assert.True(t, errors.Is(wrappedErr, pgErr1))

	// Should not match a different PgError
	assert.False(t, errors.Is(wrappedErr, pgErr2))
}

func TestPgErrorAllFields(t *testing.T) {
	// Test with all 14 PostgreSQL error fields populated
	diag := &sqltypes.PgDiagnostic{
		MessageType:      protocol.MsgErrorResponse,
		Severity:         "ERROR",
		Code:             "23505",
		Message:          "duplicate key value violates unique constraint",
		Detail:           "Key (id)=(1) already exists.",
		Hint:             "Use a different key value.",
		Position:         25,
		InternalPosition: 10,
		InternalQuery:    "SELECT internal_func()",
		Where:            "SQL function \"my_func\" statement 1",
		Schema:           "public",
		Table:            "users",
		Column:           "id",
		DataType:         "integer",
		Constraint:       "users_pkey",
	}

	pgErr := NewPgError(diag)
	result := pgErr.Diagnostic()

	// Verify all fields are preserved
	assert.Equal(t, byte(protocol.MsgErrorResponse), result.MessageType)
	assert.Equal(t, "ERROR", result.Severity)
	assert.Equal(t, "23505", result.Code)
	assert.Equal(t, "duplicate key value violates unique constraint", result.Message)
	assert.Equal(t, "Key (id)=(1) already exists.", result.Detail)
	assert.Equal(t, "Use a different key value.", result.Hint)
	assert.Equal(t, int32(25), result.Position)
	assert.Equal(t, int32(10), result.InternalPosition)
	assert.Equal(t, "SELECT internal_func()", result.InternalQuery)
	assert.Equal(t, "SQL function \"my_func\" statement 1", result.Where)
	assert.Equal(t, "public", result.Schema)
	assert.Equal(t, "users", result.Table)
	assert.Equal(t, "id", result.Column)
	assert.Equal(t, "integer", result.DataType)
	assert.Equal(t, "users_pkey", result.Constraint)
}
