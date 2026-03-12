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

package queryserving

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
)

// TestListenNotify verifies LISTEN/NOTIFY works through the multigateway.
func TestListenNotify(t *testing.T) {
	setup := getSharedSetup(t)
	setup.SetupTest(t)

	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable")

	t.Run("basic_listen_notify", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		listener, err := pgx.Connect(ctx, connStr)
		require.NoError(t, err)
		defer listener.Close(ctx)

		notifier, err := pgx.Connect(ctx, connStr)
		require.NoError(t, err)
		defer notifier.Close(ctx)

		_, err = listener.Exec(ctx, "LISTEN test_channel")
		require.NoError(t, err)

		_, err = notifier.Exec(ctx, "NOTIFY test_channel, 'hello'")
		require.NoError(t, err)

		notification, err := listener.WaitForNotification(ctx)
		require.NoError(t, err)
		assert.Equal(t, "test_channel", notification.Channel)
		assert.Equal(t, "hello", notification.Payload)
	})

	t.Run("notify_with_empty_payload", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		listener, err := pgx.Connect(ctx, connStr)
		require.NoError(t, err)
		defer listener.Close(ctx)

		notifier, err := pgx.Connect(ctx, connStr)
		require.NoError(t, err)
		defer notifier.Close(ctx)

		_, err = listener.Exec(ctx, "LISTEN empty_payload")
		require.NoError(t, err)

		_, err = notifier.Exec(ctx, "NOTIFY empty_payload")
		require.NoError(t, err)

		notification, err := listener.WaitForNotification(ctx)
		require.NoError(t, err)
		assert.Equal(t, "empty_payload", notification.Channel)
		assert.Equal(t, "", notification.Payload)
	})

	t.Run("multiple_channels", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		listener, err := pgx.Connect(ctx, connStr)
		require.NoError(t, err)
		defer listener.Close(ctx)

		notifier, err := pgx.Connect(ctx, connStr)
		require.NoError(t, err)
		defer notifier.Close(ctx)

		_, err = listener.Exec(ctx, "LISTEN chan_a")
		require.NoError(t, err)
		_, err = listener.Exec(ctx, "LISTEN chan_b")
		require.NoError(t, err)

		_, err = notifier.Exec(ctx, "NOTIFY chan_a, 'from_a'")
		require.NoError(t, err)
		_, err = notifier.Exec(ctx, "NOTIFY chan_b, 'from_b'")
		require.NoError(t, err)

		n1, err := listener.WaitForNotification(ctx)
		require.NoError(t, err)
		n2, err := listener.WaitForNotification(ctx)
		require.NoError(t, err)

		channels := map[string]string{n1.Channel: n1.Payload, n2.Channel: n2.Payload}
		assert.Equal(t, "from_a", channels["chan_a"])
		assert.Equal(t, "from_b", channels["chan_b"])
	})

	t.Run("unlisten", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		listener, err := pgx.Connect(ctx, connStr)
		require.NoError(t, err)
		defer listener.Close(ctx)

		notifier, err := pgx.Connect(ctx, connStr)
		require.NoError(t, err)
		defer notifier.Close(ctx)

		_, err = listener.Exec(ctx, "LISTEN unlisten_test")
		require.NoError(t, err)

		_, err = listener.Exec(ctx, "UNLISTEN unlisten_test")
		require.NoError(t, err)

		_, err = notifier.Exec(ctx, "NOTIFY unlisten_test, 'should_not_arrive'")
		require.NoError(t, err)

		timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 2*time.Second)
		defer timeoutCancel()
		_, err = listener.WaitForNotification(timeoutCtx)
		assert.Error(t, err, "should not receive notification after UNLISTEN")
	})

	t.Run("unlisten_all", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		listener, err := pgx.Connect(ctx, connStr)
		require.NoError(t, err)
		defer listener.Close(ctx)

		notifier, err := pgx.Connect(ctx, connStr)
		require.NoError(t, err)
		defer notifier.Close(ctx)

		_, err = listener.Exec(ctx, "LISTEN ua_chan1")
		require.NoError(t, err)
		_, err = listener.Exec(ctx, "LISTEN ua_chan2")
		require.NoError(t, err)

		_, err = listener.Exec(ctx, "UNLISTEN *")
		require.NoError(t, err)

		_, err = notifier.Exec(ctx, "NOTIFY ua_chan1")
		require.NoError(t, err)
		_, err = notifier.Exec(ctx, "NOTIFY ua_chan2")
		require.NoError(t, err)

		timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 2*time.Second)
		defer timeoutCancel()
		_, err = listener.WaitForNotification(timeoutCtx)
		assert.Error(t, err, "should not receive notifications after UNLISTEN *")
	})

	t.Run("listen_in_transaction_commit", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		listener, err := pgx.Connect(ctx, connStr)
		require.NoError(t, err)
		defer listener.Close(ctx)

		notifier, err := pgx.Connect(ctx, connStr)
		require.NoError(t, err)
		defer notifier.Close(ctx)

		_, err = listener.Exec(ctx, "BEGIN")
		require.NoError(t, err)
		_, err = listener.Exec(ctx, "LISTEN txn_channel")
		require.NoError(t, err)

		// Notify before commit — listener shouldn't receive yet (LISTEN not committed)
		_, err = notifier.Exec(ctx, "NOTIFY txn_channel, 'before_commit'")
		require.NoError(t, err)

		_, err = listener.Exec(ctx, "COMMIT")
		require.NoError(t, err)

		// Send after commit
		_, err = notifier.Exec(ctx, "NOTIFY txn_channel, 'after_commit'")
		require.NoError(t, err)

		notification, err := listener.WaitForNotification(ctx)
		require.NoError(t, err)
		assert.Equal(t, "txn_channel", notification.Channel)
		assert.Equal(t, "after_commit", notification.Payload)
	})

	t.Run("listen_in_transaction_rollback", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		listener, err := pgx.Connect(ctx, connStr)
		require.NoError(t, err)
		defer listener.Close(ctx)

		notifier, err := pgx.Connect(ctx, connStr)
		require.NoError(t, err)
		defer notifier.Close(ctx)

		_, err = listener.Exec(ctx, "BEGIN")
		require.NoError(t, err)
		_, err = listener.Exec(ctx, "LISTEN rollback_channel")
		require.NoError(t, err)
		_, err = listener.Exec(ctx, "ROLLBACK")
		require.NoError(t, err)

		_, err = notifier.Exec(ctx, "NOTIFY rollback_channel, 'should_not_arrive'")
		require.NoError(t, err)

		timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 2*time.Second)
		defer timeoutCancel()
		_, err = listener.WaitForNotification(timeoutCtx)
		assert.Error(t, err, "should not receive notification after ROLLBACK")
	})

	t.Run("multiple_listeners_same_channel", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		listener1, err := pgx.Connect(ctx, connStr)
		require.NoError(t, err)
		defer listener1.Close(ctx)

		listener2, err := pgx.Connect(ctx, connStr)
		require.NoError(t, err)
		defer listener2.Close(ctx)

		notifier, err := pgx.Connect(ctx, connStr)
		require.NoError(t, err)
		defer notifier.Close(ctx)

		_, err = listener1.Exec(ctx, "LISTEN shared_channel")
		require.NoError(t, err)
		_, err = listener2.Exec(ctx, "LISTEN shared_channel")
		require.NoError(t, err)

		_, err = notifier.Exec(ctx, "NOTIFY shared_channel, 'broadcast'")
		require.NoError(t, err)

		n1, err := listener1.WaitForNotification(ctx)
		require.NoError(t, err)
		assert.Equal(t, "broadcast", n1.Payload)

		n2, err := listener2.WaitForNotification(ctx)
		require.NoError(t, err)
		assert.Equal(t, "broadcast", n2.Payload)
	})

	t.Run("notify_via_pg_notify", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		listener, err := pgx.Connect(ctx, connStr)
		require.NoError(t, err)
		defer listener.Close(ctx)

		notifier, err := pgx.Connect(ctx, connStr)
		require.NoError(t, err)
		defer notifier.Close(ctx)

		_, err = listener.Exec(ctx, "LISTEN pg_notify_test")
		require.NoError(t, err)

		_, err = notifier.Exec(ctx, "SELECT pg_notify('pg_notify_test', 'via_function')")
		require.NoError(t, err)

		notification, err := listener.WaitForNotification(ctx)
		require.NoError(t, err)
		assert.Equal(t, "pg_notify_test", notification.Channel)
		assert.Equal(t, "via_function", notification.Payload)
	})

	t.Run("rapid_notifications", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		listener, err := pgx.Connect(ctx, connStr)
		require.NoError(t, err)
		defer listener.Close(ctx)

		notifier, err := pgx.Connect(ctx, connStr)
		require.NoError(t, err)
		defer notifier.Close(ctx)

		_, err = listener.Exec(ctx, "LISTEN rapid")
		require.NoError(t, err)

		count := 100
		for i := 0; i < count; i++ {
			_, err = notifier.Exec(ctx, fmt.Sprintf("NOTIFY rapid, '%d'", i))
			require.NoError(t, err)
		}

		received := 0
		for received < count {
			_, err := listener.WaitForNotification(ctx)
			require.NoError(t, err)
			received++
		}
		assert.Equal(t, count, received)
	})
}
