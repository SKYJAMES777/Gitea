// Copyright 2020 The Gitea Authors. All rights reserved.
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

	"code.gitea.io/gitea/models"
	"code.gitea.io/gitea/modules/log"
	"code.gitea.io/gitea/modules/queue"
	"code.gitea.io/gitea/modules/setting"
)

// Deliver delivers a webhook event to the target URL.
func Deliver(ctx context.Context, w *models.Webhook, payloader queue.Payloader) error {
	// Create a context with timeout for the HTTP request
	reqCtx, cancel := context.WithTimeout(ctx, setting.Webhook.DeliverTimeout)
	defer cancel()

	// Build the request
	payload, err := payloader.JSONPayload()
	if err != nil {
		return fmt.Errorf("Deliver: JSONPayload: %v", err)
	}

	req, err := http.NewRequestWithContext(reqCtx, "POST", w.URL, strings.NewReader(payload))
	if err != nil {
		return fmt.Errorf("Deliver: NewRequest: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Gitea-Event", payloader.Event())
	req.Header.Set("X-Gitea-Delivery", payloader.UUID())

	// Set headers from webhook
	for k, v := range w.Header {
		req.Header[k] = v
	}

	// Send the request
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("Deliver: Do: %v", err)
	}
	defer resp.Body.Close()

	// Check for rate limit (HTTP 429)
	if resp.StatusCode == http.StatusTooManyRequests {
		// Parse Retry-After header
		retryAfter := resp.Header.Get("Retry-After")
		var waitDuration time.Duration
		if retryAfter != "" {
			// Try to parse as seconds
			if seconds, err := strconv.Atoi(retryAfter); err == nil {
				waitDuration = time.Duration(seconds) * time.Second
			} else if retryTime, err := time.Parse(time.RFC1123, retryAfter); err == nil {
				waitDuration = time.Until(retryTime)
			} else {
				// Default to 1 minute if header is malformed
				waitDuration = 1 * time.Minute
			}
		} else {
			// Default to 1 minute if no Retry-After header
			waitDuration = 1 * time.Minute
		}

		// Cap the wait duration to avoid excessive blocking
		if waitDuration > 5*time.Minute {
			waitDuration = 5 * time.Minute
		}

		// Use a select with context to avoid blocking on shutdown
		select {
		case <-time.After(waitDuration):
			// Retry after waiting
			return Deliver(ctx, w, payloader)
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Read and discard the response body to reuse connections
	_, _ = io.Copy(io.Discard, resp.Body)

	// Check for successful status code
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("Deliver: unexpected status %d", resp.StatusCode)
	}

	return nil
}

// DeliverWorker is the worker that processes webhook deliveries from the queue.
func DeliverWorker(ctx context.Context, wg *sync.WaitGroup, queue queue.Queue) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			log.Info("Webhook delivery worker shutting down")
			return
		default:
		}

		// Get a task from the queue
		task, err := queue.Pop(ctx)
		if err != nil {
			if err == queue.ErrQueueEmpty {
				// No tasks, wait a bit before polling again
				time.Sleep(100 * time.Millisecond)
				continue
			}
			log.Error("Webhook delivery worker: failed to pop from queue: %v", err)
			continue
		}

		// Process the task
		if err := processTask(ctx, task); err != nil {
			log.Error("Webhook delivery worker: failed to process task: %v", err)
		}
	}
}

func processTask(ctx context.Context, task queue.Task) error {
	// Type assert the task to a webhook payload
	payload, ok := task.(queue.Payloader)
	if !ok {
		return fmt.Errorf("processTask: unexpected task type %T", task)
	}

	// Get the webhook from the database
	w, err := models.GetWebhookByID(payload.WebhookID())
	if err != nil {
		return fmt.Errorf("processTask: GetWebhookByID: %v", err)
	}

	// Deliver the webhook
	return Deliver(ctx, w, payload)
}
