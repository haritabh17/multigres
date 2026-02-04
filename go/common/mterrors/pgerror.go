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

	"github.com/multigres/multigres/go/common/sqltypes"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// PgError represents a PostgreSQL error with full diagnostic information.
// It wraps sqltypes.PgDiagnostic to preserve all 14 PostgreSQL error fields
// as they pass through the Multigres system.
//
// PgError should NOT be wrapped with mterrors.Wrapf() - it should pass through
// unchanged to preserve the original PostgreSQL error format for the client.
type PgError struct {
	diag  *sqltypes.PgDiagnostic
	stack *stack
}

// NewPgError creates a new PgError from a *sqltypes.PgDiagnostic.
// Since sqltypes.PgDiagnostic implements the error interface directly,
// this function accepts it as the canonical PostgreSQL error type.
func NewPgError(diag *sqltypes.PgDiagnostic) *PgError {
	return &PgError{
		diag:  diag,
		stack: callers(),
	}
}

// NewPgErrorFromDiagnostic creates a new PgError from a PgDiagnostic.
// Use this when reconstructing a PgError from gRPC responses.
func NewPgErrorFromDiagnostic(diag *sqltypes.PgDiagnostic) *PgError {
	return &PgError{
		diag:  diag,
		stack: callers(),
	}
}

// Error implements the error interface.
// Returns a clean PostgreSQL-style error message without wrapping prefixes.
// Format: "SEVERITY: message"
// Use FullError() to include the SQLSTATE code for debugging.
// Safe to call on nil receiver.
func (e *PgError) Error() string {
	if e == nil || e.diag == nil {
		return "ERROR: unknown error"
	}
	return e.diag.Severity + ": " + e.diag.Message
}

// FullError returns the error with SQLSTATE code for debugging purposes.
// Format: "SEVERITY: message (SQLSTATE code)"
// Safe to call on nil receiver.
func (e *PgError) FullError() string {
	if e == nil || e.diag == nil {
		return "ERROR: unknown error (SQLSTATE 00000)"
	}
	return e.diag.Severity + ": " + e.diag.Message + " (SQLSTATE " + e.diag.Code + ")"
}

// Diagnostic returns the underlying PgDiagnostic.
// Use this to access all 14 PostgreSQL error fields for wire serialization.
// Safe to call on nil receiver; returns nil.
func (e *PgError) Diagnostic() *sqltypes.PgDiagnostic {
	if e == nil {
		return nil
	}
	return e.diag
}

// ErrorCode implements ErrorWithCode interface.
// Returns UNKNOWN since PostgreSQL errors don't map directly to gRPC codes.
// The actual error categorization is done via the SQLSTATE code (Diagnostic().Code).
// Safe to call on nil receiver.
func (e *PgError) ErrorCode() mtrpcpb.Code {
	// Always return UNKNOWN, even for nil receiver
	return mtrpcpb.Code_UNKNOWN
}

// Unwrap implements the error unwrapping interface for use with errors.Is() and errors.As().
// PgError is intentionally a leaf error - it represents the original PostgreSQL error
// and has no underlying cause. This method returns nil to indicate that PgError
// terminates the error chain.
//
// This allows errors.As(err, &pgErr) to work correctly when PgError is wrapped
// with fmt.Errorf("%w", pgErr) or mterrors.Wrapf().
func (e *PgError) Unwrap() error {
	return nil
}

// IsPgError checks if err is or wraps a PgError.
func IsPgError(err error) bool {
	var pgErr *PgError
	return errors.As(err, &pgErr)
}

// AsPgError extracts a PgError from err if present.
// Returns the PgError and true if found, nil and false otherwise.
func AsPgError(err error) (*PgError, bool) {
	var pgErr *PgError
	if errors.As(err, &pgErr) {
		return pgErr, true
	}
	return nil, false
}
