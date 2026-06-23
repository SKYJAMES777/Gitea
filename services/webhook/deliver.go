// Copyright 2024 The Gitea Authors. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package webhook

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"code.gitea.io/gitea/models/webhook"
	"code.gitea.io/gitea/modules/log"
	"code.gitea.io/gitea/modules/setting"
	"code.gitea.io/gitea/modules/timeutil"
)

// Deliver delivers a webhook event to the target URL.
func Deliver(ctx context.Context, w *webhook.Webhook, payloader webhook.Payloader, event webhook.HookEventType, signature string) error {
	// Create a context with a strict timeout to prevent hanging connections
	deliverCtx, cancel := context.WithTimeout(ctx, setting.Webhook.DeliverTimeout)
	defer cancel()

	// Build the request
	payload, err := payloader.JSONPayload()
	if err != nil {
		return fmt.Errorf("JSONPayload: %w", err)
	}

	req, err := http.NewRequestWithContext(deliverCtx, http.MethodPost, w.URL, strings.NewReader(string(payload)))
	if err != nil {
		return fmt.Errorf("NewRequest: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Gitea-Event", string(event))
	req.Header.Set("X-Gitea-Signature", signature)
	req.Header.Set("X-Gitea-Delivery", w.UUID)

	// Send the request
	client := &http.Client{
		Timeout: setting.Webhook.DeliverTimeout,
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("Do: %w", err)
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("ReadAll: %w", err)
	}

	// Check for rate-limit (429) response
	if resp.StatusCode == http.StatusTooManyRequests {
		// Parse Retry-After header
		retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
		if retryAfter > 0 {
			// Use a separate goroutine to wait and then re-enqueue the webhook
			// This prevents blocking the current worker
			go func() {
				timer := time.NewTimer(retryAfter)
				defer timer.Stop()

				select {
				case <-timer.C:
					// Re-enqueue the webhook for delivery
					if err := webhook.Enqueue(w.ID); err != nil {
						log.Error("Failed to re-enqueue webhook %d after rate limit: %v", w.ID, err)
					}
				case <-ctx.Done():
					// Context cancelled (e.g., shutdown), do nothing
					log.Warn("Webhook %d delivery cancelled during rate-limit backoff", w.ID)
				}
			}()
			return nil // Return nil to indicate the webhook was handled (re-enqueued)
		}
	}

	// Log the response
	log.Trace("Webhook %s delivered to %s with status %d: %s", w.UUID, w.URL, resp.StatusCode, string(body))

	// Update the webhook's last delivery status
	if err := webhook.UpdateDeliveryStatus(w.ID, resp.StatusCode, string(body)); err != nil {
		log.Error("Failed to update delivery status for webhook %d: %v", w.ID, err)
	}

	return nil
}

// parseRetryAfter parses the Retry-After header value and returns the duration to wait.
// It supports both seconds (integer) and HTTP-date formats.
func parseRetryAfter(val string) time.Duration {
	if val == "" {
		return 0
	}

	// Try to parse as seconds (integer)
	if seconds, err := strconv.Atoi(val); err == nil {
		return time.Duration(seconds) * time.Second
	}

	// Try to parse as HTTP-date
	if t, err := time.Parse(time.RFC1123, val); err == nil {
		return time.Until(t)
	}

	// Fallback: return a default backoff of 60 seconds
	return 60 * time.Second
}

// HookQueue is the queue for webhook deliveries
type HookQueue struct {
	queue chan int64
	wg    sync.WaitGroup
}

// NewHookQueue creates a new webhook delivery queue
func NewHookQueue() *HookQueue {
	return &HookQueue{
		queue: make(chan int64, 100),
	}
}

// Run starts the webhook delivery worker pool
func (hq *HookQueue) Run(ctx context.Context, numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		hq.wg.Add(1)
		go hq.worker(ctx)
	}
}

// Shutdown waits for all workers to finish
func (hq *HookQueue) Shutdown() {
	hq.wg.Wait()
}

// Enqueue adds a webhook ID to the delivery queue
func (hq *HookQueue) Enqueue(webhookID int64) {
	select {
	case hq.queue <- webhookID:
	default:
		log.Warn("Webhook delivery queue is full, dropping webhook %d", webhookID)
	}
}

// worker processes webhook deliveries from the queue
func (hq *HookQueue) worker(ctx context.Context) {
	defer hq.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case webhookID := <-hq.queue:
			// Fetch the webhook from the database
			w, err := webhook.GetByID(webhookID)
			if err != nil {
				log.Error("Failed to get webhook %d: %v", webhookID, err)
				continue
			}

			// Deliver the webhook
			if err := Deliver(ctx, w, nil, "", ""); err != nil {
				log.Error("Failed to deliver webhook %d: %v", webhookID, err)
			}
		}
	}
}
