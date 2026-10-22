package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const maxPingResponseSize = 64 * 1024

type Client struct {
	httpClient *http.Client
}

func NewClient() *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *Client) Ping(
	ctx context.Context,
	address string,
	req PingRequest,
) (PingResponse, error) {
	if req.From == "" {
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
		return PingResponse{}, fmt.Errorf(
			"ping peer %s: %w",
			address,
			err,
		)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(
			io.LimitReader(resp.Body, maxPingResponseSize),
		)

		return PingResponse{}, fmt.Errorf(
			"ping peer %s: unexpected HTTP status %d: %s",
			address,
			resp.StatusCode,
			string(body),
		)
	}

	var pingResp PingResponse

	decoder := json.NewDecoder(
		io.LimitReader(resp.Body, maxPingResponseSize+1),
	)

	if err := decoder.Decode(&pingResp); err != nil {
		return PingResponse{}, fmt.Errorf(
			"decode ping response from %s: %w",
			address,
			err,
		)
	}

	if pingResp.NodeID == "" {
		return PingResponse{}, fmt.Errorf(
			"ping peer %s: missing node ID in response",
			address,
		)
	}

	return pingResp, nil
}
