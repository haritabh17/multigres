// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package poolergateway

import (
	"context"
	"log/slog"
	"sync"

	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// GRPCNotificationManager implements handler.NotificationManager by calling
// the pooler's StreamNotifications gRPC. It manages per-channel streams
// and fans out notifications to subscriber channels.
type GRPCNotificationManager struct {
	getClient func() multipoolerpb.MultiPoolerServiceClient
	logger    *slog.Logger

	mu          sync.Mutex
	// channels tracks: pgChannel -> list of subscriber notifCh
	channels    map[string][]chan *handler.Notification
	// streams tracks: pgChannel -> cancel func for the gRPC stream
	streams     map[string]context.CancelFunc
}

// NewGRPCNotificationManager creates a notification manager backed by gRPC.
func NewGRPCNotificationManager(
	getClient func() multipoolerpb.MultiPoolerServiceClient,
	logger *slog.Logger,
) *GRPCNotificationManager {
	return &GRPCNotificationManager{
		getClient: getClient,
		logger:    logger,
		channels:  make(map[string][]chan *handler.Notification),
		streams:   make(map[string]context.CancelFunc),
	}
}

// Subscribe registers notifCh to receive notifications for pgChannel.
func (m *GRPCNotificationManager) Subscribe(pgChannel string, notifCh chan *handler.Notification) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.channels[pgChannel] = append(m.channels[pgChannel], notifCh)

	// If this is the first subscriber for this channel, start a gRPC stream.
	if len(m.channels[pgChannel]) == 1 {
		ctx, cancel := context.WithCancel(context.Background())
		m.streams[pgChannel] = cancel
		go m.streamNotifications(ctx, pgChannel)
	}
}

// Unsubscribe removes notifCh from pgChannel subscribers.
func (m *GRPCNotificationManager) Unsubscribe(pgChannel string, notifCh chan *handler.Notification) {
	m.mu.Lock()
	defer m.mu.Unlock()

	subs := m.channels[pgChannel]
	for i, ch := range subs {
		if ch == notifCh {
			m.channels[pgChannel] = append(subs[:i], subs[i+1:]...)
			break
		}
	}

	// If no more subscribers, cancel the gRPC stream.
	if len(m.channels[pgChannel]) == 0 {
		delete(m.channels, pgChannel)
		if cancel, ok := m.streams[pgChannel]; ok {
			cancel()
			delete(m.streams, pgChannel)
		}
	}
}

// UnsubscribeAll removes notifCh from all channels.
func (m *GRPCNotificationManager) UnsubscribeAll(notifCh chan *handler.Notification) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for pgChannel, subs := range m.channels {
		for i, ch := range subs {
			if ch == notifCh {
				m.channels[pgChannel] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
		if len(m.channels[pgChannel]) == 0 {
			delete(m.channels, pgChannel)
			if cancel, ok := m.streams[pgChannel]; ok {
				cancel()
				delete(m.streams, pgChannel)
			}
		}
	}
}

// streamNotifications opens a StreamNotifications gRPC stream and fans out
// notifications to all subscribers for the given channel.
func (m *GRPCNotificationManager) streamNotifications(ctx context.Context, pgChannel string) {
	client := m.getClient()
	if client == nil {
		m.logger.Error("no gRPC client available for notifications")
		return
	}

	stream, err := client.StreamNotifications(ctx, &multipoolerpb.StreamNotificationsRequest{
		Channels: []string{pgChannel},
	})
	if err != nil {
		m.logger.Error("failed to open notification stream", "channel", pgChannel, "error", err)
		return
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return // cancelled
			}
			m.logger.Error("notification stream error", "channel", pgChannel, "error", err)
			return
		}

		var notif *handler.Notification
		if resp.IsWarning {
			notif = &handler.Notification{
				IsWarning:      true,
				WarningMessage: resp.WarningMessage,
			}
		} else if resp.Notification != nil {
			notif = &handler.Notification{
				PID:     resp.Notification.Pid,
				Channel: resp.Notification.Channel,
				Payload: resp.Notification.Payload,
			}
		}

		if notif != nil {
			m.mu.Lock()
			for _, ch := range m.channels[pgChannel] {
				select {
				case ch <- notif:
				default:
					m.logger.Warn("notification channel full", "channel", pgChannel)
				}
			}
			m.mu.Unlock()
		}
	}
}
