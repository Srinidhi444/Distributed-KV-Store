package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const maxPingResponseSize = 64 * 1024

type Client struct {
	httpClient *http.Client
}

func NewClient() *Client {
	return NewClientWithTimeout(10 * time.Second)
}

func (c *Client) Ping(
	ctx context.Context,
	address string,
	req PingRequest,
) (PingResponse, error) {
	if strings.TrimSpace(req.From) == "" {
		return PingResponse{}, fmt.Errorf("ping sender cannot be empty")
	}

	payload, err := json.Marshal(req)
	if err != nil {
		return PingResponse{}, fmt.Errorf("marshal ping request: %w", err)
	}

	url := fmt.Sprintf("http://%s/internal/ping", address)

	httpReq, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		url,
		bytes.NewReader(payload),
	)
	if err != nil {
		return PingResponse{}, fmt.Errorf("create ping request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		// Retain original meaning for caller cancellations and deadlines
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return PingResponse{}, err
		}
		if ctx.Err() != nil {
			return PingResponse{}, ctx.Err()
		}
		// Wrap connection/network errors with ErrPeerUnavailable
		return PingResponse{}, errors.Join(ErrPeerUnavailable, err)
	}
	defer resp.Body.Close()

	// Handle non-200 HTTP responses
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxPingResponseSize))
		return PingResponse{}, fmt.Errorf("%w: HTTP status %d: %s", ErrUnexpectedStatus, resp.StatusCode, string(body))
	}

	// Read up to maxPingResponseSize + 1 bytes to detect size violations deterministically
	bodyBytes, err := io.ReadAll(io.LimitReader(resp.Body, maxPingResponseSize+1))
	if err != nil {
		return PingResponse{}, errors.Join(ErrInvalidResponse, err)
	}
	if int64(len(bodyBytes)) > maxPingResponseSize {
		return PingResponse{}, fmt.Errorf("%w: response exceeds maximum allowed size", ErrInvalidResponse)
	}

	// Decode and strictly validate the JSON response
	var pingResp PingResponse
	decoder := json.NewDecoder(bytes.NewReader(bodyBytes))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&pingResp); err != nil {
		return PingResponse{}, errors.Join(ErrInvalidResponse, err)
	}

	// Reject trailing/multiple JSON objects
	if decoder.More() {
		return PingResponse{}, fmt.Errorf("%w: multiple JSON objects found in response", ErrInvalidResponse)
	}

	// Validate fields
	if strings.TrimSpace(pingResp.NodeID) == "" {
		return PingResponse{}, fmt.Errorf("%w: missing or empty node ID", ErrInvalidResponse)
	}
	if pingResp.Message != "pong" {
		return PingResponse{}, fmt.Errorf("%w: unexpected message field: %q", ErrInvalidResponse, pingResp.Message)
	}

	return pingResp, nil
}

func NewClientWithTimeout(timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	httpClient := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	return &Client{
		httpClient: httpClient,
	}
}
