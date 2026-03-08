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

package handler

import (
	"context"
	"fmt"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multipooler/connstate"
)

// handleSQLPrepare handles SQL-level PREPARE statements.
// Stores the prepared statement in the consolidator and adds it to session
// settings so the multipooler can ensure it exists on any backend connection.
// The PREPARE is handled locally — no round trip to PostgreSQL.
//
// Known limitations:
//   - Validation errors (e.g., nonexistent table) are deferred to EXECUTE time
//   - pg_prepared_statements.statement shows query only (not full PREPARE ... AS ...)
//   - Transaction rollback does not undo PREPARE
func (h *MultiGatewayHandler) handleSQLPrepare(
	ctx context.Context,
	conn *server.Conn,
	stmt *ast.PrepareStmt,
	callback func(ctx context.Context, result *sqltypes.Result) error,
) error {
	// Deparse the inner query from the AST.
	innerQuery := stmt.Query.(interface{ SqlString() string }).SqlString()

	// Extract parameter type OIDs. Use 0 (unspecified) for all params,
	// letting PostgreSQL infer types when the statement is parsed on a backend.
	var paramTypes []uint32
	if stmt.Argtypes != nil && stmt.Argtypes.Len() > 0 {
		paramTypes = make([]uint32, stmt.Argtypes.Len())
	}

	// Store in consolidator — this also validates the SQL can be parsed.
	_, err := h.psc.AddPreparedStatement(conn.ConnectionID(), stmt.Name, innerQuery, paramTypes)
	if err != nil {
		return mterrors.NewPgError("ERROR", "42P05",
			fmt.Sprintf("prepared statement \"%s\" already exists", stmt.Name), "")
	}

	// Add to session settings so the multipooler ensures this statement
	// is prepared on whatever backend connection handles subsequent queries.
	state := h.getConnectionState(conn)
	state.SetSessionVariable(connstate.PreparedStatementSettingsPrefix+stmt.Name, innerQuery)

	return callback(ctx, &sqltypes.Result{CommandTag: "PREPARE"})
}

// handleSQLDeallocate handles SQL-level DEALLOCATE statements.
// Removes the prepared statement from the consolidator and session settings.
// The multipooler will deallocate it on backend connections when it syncs state.
//
// Known limitations:
//   - Transaction rollback does not undo DEALLOCATE
func (h *MultiGatewayHandler) handleSQLDeallocate(
	ctx context.Context,
	conn *server.Conn,
	stmt *ast.DeallocateStmt,
	callback func(ctx context.Context, result *sqltypes.Result) error,
) error {
	state := h.getConnectionState(conn)

	if stmt.IsAll {
		// Remove all prepared statements from consolidator and session settings.
		h.removeAllPreparedFromSession(conn, state)
	} else {
		psi := h.psc.GetPreparedStatementInfo(conn.ConnectionID(), stmt.Name)
		if psi == nil {
			return mterrors.NewPgError("ERROR", "26000",
				fmt.Sprintf("prepared statement \"%s\" does not exist", stmt.Name), "")
		}
		h.psc.RemovePreparedStatement(conn.ConnectionID(), stmt.Name)
		state.ResetSessionVariable(connstate.PreparedStatementSettingsPrefix + stmt.Name)
	}

	return callback(ctx, &sqltypes.Result{CommandTag: "DEALLOCATE"})
}

// removeAllPreparedFromSession removes all prepared statement entries from
// the session settings and consolidator for this connection.
func (h *MultiGatewayHandler) removeAllPreparedFromSession(conn *server.Conn, state *MultiGatewayConnectionState) {
	// Remove from session settings
	settings := state.GetSessionSettings()
	for key := range settings {
		if isPreparedStatementSetting(key) {
			state.ResetSessionVariable(key)
		}
	}
	// Remove from consolidator
	h.psc.RemoveConnection(conn.ConnectionID())
}

// isSQLPrepareExecute checks if the statement is a SQL-level PREPARE or
// DEALLOCATE that should be intercepted by the handler.
// EXECUTE is NOT intercepted — it flows through as raw SQL to PostgreSQL.
func isSQLPrepareOrDeallocate(stmt ast.Stmt) bool {
	switch stmt.NodeTag() {
	case ast.T_PrepareStmt, ast.T_DeallocateStmt:
		return true
	}
	return false
}

// executeSQLPrepareOrDeallocate dispatches to the appropriate handler method.
func (h *MultiGatewayHandler) executeSQLPrepareOrDeallocate(
	ctx context.Context,
	conn *server.Conn,
	stmt ast.Stmt,
	callback func(ctx context.Context, result *sqltypes.Result) error,
) error {
	switch s := stmt.(type) {
	case *ast.PrepareStmt:
		return h.handleSQLPrepare(ctx, conn, s, callback)
	case *ast.DeallocateStmt:
		return h.handleSQLDeallocate(ctx, conn, s, callback)
	default:
		return fmt.Errorf("unexpected statement type: %T", stmt)
	}
}

// isPreparedStatementSetting returns true if the session settings key
// represents a prepared statement (has the __ps: prefix).
func isPreparedStatementSetting(key string) bool {
	return len(key) > len(connstate.PreparedStatementSettingsPrefix) &&
		key[:len(connstate.PreparedStatementSettingsPrefix)] == connstate.PreparedStatementSettingsPrefix
}
