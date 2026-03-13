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

// Package pubsub implements a shared PG LISTEN/NOTIFY listener
// that fans out notifications to multiple gateway client sessions.
package pubsub

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/sqltypes"
)

// Notification wraps a PG notification with optional warning info.
type Notification struct {
	PID            int32
	Channel        string
	Payload        string
	IsWarning      bool
	WarningMessage string
}

// request types for the serialized event loop.
type requestType int

const (
	reqSubscribe requestType = iota
	reqUnsubscribe
	reqUnsubscribeAll
)

type request struct {
	typ     requestType
	channel string
	subCh   chan *Notification
	done    chan struct{} // closed when request is processed
}

// Listener manages a single dedicated PG connection for LISTEN/NOTIFY,
// with channel refcounting and fan-out to subscribers.
type Listener struct {
	config *client.Config
	logger *slog.Logger

	// requests is the serialized command channel for the event loop.
	requests chan request

	// cancel stops the background goroutines.
	cancel context.CancelFunc

	// wg tracks background goroutines.
	wg sync.WaitGroup
}

// NewListener creates a new PubSubListener. Call Start() to begin.
func NewListener(config *client.Config, logger *slog.Logger) *Listener {
	return &Listener{
		config:   config,
		logger:   logger,
		requests: make(chan request, 64),
	}
}

// Start begins the background event loop.
func (l *Listener) Start(ctx context.Context) {
	ctx, l.cancel = context.WithCancel(ctx)
	l.wg.Add(1)
	go l.run(ctx)
}

// Stop shuts down the listener and waits for goroutines to exit.
func (l *Listener) Stop() {
	if l.cancel != nil {
		l.cancel()
	}
	l.wg.Wait()
}

// Subscribe registers a subscriber for the given channel.
// Returns a channel that will receive notifications.
// The returned channel has buffer of 64 to avoid blocking the fan-out.
func (l *Listener) Subscribe(channel string) chan *Notification {
	ch := make(chan *Notification, 64)
	req := request{
		typ:     reqSubscribe,
		channel: channel,
		subCh:   ch,
		done:    make(chan struct{}),
	}
	l.requests <- req
	<-req.done
	return ch
}

// Unsubscribe removes a subscriber from the given channel.
func (l *Listener) Unsubscribe(channel string, subCh chan *Notification) {
	req := request{
		typ:     reqUnsubscribe,
		channel: channel,
		subCh:   subCh,
		done:    make(chan struct{}),
	}
	l.requests <- req
	<-req.done
}

// UnsubscribeAll removes a subscriber from all channels.
func (l *Listener) UnsubscribeAll(subCh chan *Notification) {
	req := request{
		typ:   reqUnsubscribeAll,
		subCh: subCh,
		done:  make(chan struct{}),
	}
	l.requests <- req
	<-req.done
}

// run is the main event loop. It manages the PG connection, reads notifications,
// and processes subscribe/unsubscribe requests.
func (l *Listener) run(ctx context.Context) {
	defer l.wg.Done()

	// Channel refcounts: channel name -> number of subscribers.
	channels := make(map[string]int)
	// Subscribers: channel name -> list of subscriber channels.
	subscribers := make(map[string][]chan *Notification)
	// All subscriber channels (for broadcast warnings).
	allSubs := make(map[chan *Notification]struct{})

	var conn *client.Conn
	var notifCh chan *sqltypes.Notification // receives from reader goroutine
	var errCh chan error                    // reader goroutine error

	connect := func() {
		if conn != nil {
			conn.Close()
			conn = nil
		}
		var err error
		conn, err = client.Connect(ctx, ctx, l.config)
		if err != nil {
			l.logger.Error("pubsub: failed to connect to PG", "error", err)
			conn = nil
			return
		}
		l.logger.Info("pubsub: connected to PG")

		// Re-LISTEN all channels.
		for ch := range channels {
			_, err := conn.Query(ctx, fmt.Sprintf("LISTEN %s", quoteIdent(ch)))
			if err != nil {
				l.logger.Error("pubsub: failed to re-LISTEN", "channel", ch, "error", err)
				conn.Close()
				conn = nil
				return
			}
		}

		// Start reader goroutine.
		notifCh = make(chan *sqltypes.Notification, 64)
		errCh = make(chan error, 1)
		go func(c *client.Conn, nch chan<- *sqltypes.Notification, ech chan<- error) {
			for {
				n, err := c.WaitForNotification(ctx)
				if err != nil {
					ech <- err
					return
				}
				nch <- n
			}
		}(conn, notifCh, errCh)
	}

	connect()

	reconnectTimer := time.NewTimer(0)
	reconnectTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			if conn != nil {
				conn.Close()
			}
			// Close all subscriber channels.
			for sub := range allSubs {
				close(sub)
			}
			return

		case req := <-l.requests:
			switch req.typ {
			case reqSubscribe:
				oldCount := channels[req.channel]
				channels[req.channel]++
				subscribers[req.channel] = append(subscribers[req.channel], req.subCh)
				allSubs[req.subCh] = struct{}{}

				if oldCount == 0 && conn != nil {
					_, err := conn.Query(ctx, fmt.Sprintf("LISTEN %s", quoteIdent(req.channel)))
					if err != nil {
						l.logger.Error("pubsub: LISTEN failed", "channel", req.channel, "error", err)
					}
				}

			case reqUnsubscribe:
				l.removeSub(subscribers, allSubs, channels, req.channel, req.subCh, conn, ctx)

			case reqUnsubscribeAll:
				// Collect channels to check (can't modify map during iteration safely with removeSub)
				var toRemove []string
				for ch, subs := range subscribers {
					for _, s := range subs {
						if s == req.subCh {
							toRemove = append(toRemove, ch)
							break
						}
					}
				}
				for _, ch := range toRemove {
					l.removeSub(subscribers, allSubs, channels, ch, req.subCh, conn, ctx)
				}
			}
			close(req.done)

		case n := <-notifCh:
			if n == nil {
				continue
			}
			notif := &Notification{
				PID:     n.PID,
				Channel: n.Channel,
				Payload: n.Payload,
			}
			for _, sub := range subscribers[n.Channel] {
				select {
				case sub <- notif:
				default:
					l.logger.Warn("pubsub: subscriber channel full, dropping notification",
						"channel", n.Channel)
				}
			}

		case err := <-errCh:
			l.logger.Error("pubsub: reader error, will reconnect", "error", err)
			if conn != nil {
				conn.Close()
				conn = nil
			}
			// Send warning to all subscribers.
			warning := &Notification{
				IsWarning:      true,
				WarningMessage: "notification listener reconnected, some notifications may have been missed",
			}
			for sub := range allSubs {
				select {
				case sub <- warning:
				default:
				}
			}
			// Schedule reconnect.
			reconnectTimer.Reset(2 * time.Second)

		case <-reconnectTimer.C:
			if conn == nil && len(channels) > 0 {
				connect()
				if conn == nil {
					reconnectTimer.Reset(5 * time.Second)
				}
			}
		}
	}
}

// removeSub removes a single subscriber from a channel and UNLISTENs if refcount hits 0.
func (l *Listener) removeSub(
	subscribers map[string][]chan *Notification,
	allSubs map[chan *Notification]struct{},
	channels map[string]int,
	channel string,
	subCh chan *Notification,
	conn *client.Conn,
	ctx context.Context,
) {
	subs := subscribers[channel]
	for i, s := range subs {
		if s == subCh {
			subscribers[channel] = append(subs[:i], subs[i+1:]...)
			break
		}
	}
	delete(allSubs, subCh)

	channels[channel]--
	if channels[channel] <= 0 {
		delete(channels, channel)
		delete(subscribers, channel)
		if conn != nil {
			_, err := conn.Query(ctx, fmt.Sprintf("UNLISTEN %s", quoteIdent(channel)))
			if err != nil {
				l.logger.Error("pubsub: UNLISTEN failed", "channel", channel, "error", err)
			}
		}
	}
}

// quoteIdent quotes a PG identifier for use in LISTEN/UNLISTEN.
func quoteIdent(s string) string {
	return `"` + doubleQuoteEscape(s) + `"`
}

func doubleQuoteEscape(s string) string {
	result := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '"' {
			result = append(result, '"', '"')
		} else {
			result = append(result, s[i])
		}
	}
	return string(result)
}

// SubscribeCh registers an existing notification channel to receive notifications
// for the given PG channel name. Unlike Subscribe, this lets multiple PG channels
// feed into the same notification channel.
func (l *Listener) SubscribeCh(channel string, notifCh chan *Notification) {
	req := request{
		typ:     reqSubscribe,
		channel: channel,
		subCh:   notifCh,
		done:    make(chan struct{}),
	}
	l.requests <- req
	<-req.done
}

// UnsubscribeCh removes a subscription for a specific PG channel using the provided channel.
func (l *Listener) UnsubscribeCh(channel string, notifCh chan *Notification) {
	req := request{
		typ:     reqUnsubscribe,
		channel: channel,
		subCh:   notifCh,
		done:    make(chan struct{}),
	}
	l.requests <- req
	<-req.done
}
