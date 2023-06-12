//go:build !solution
// +build !solution

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"go.uber.org/zap"
)

type HeartbeatClient struct {
	endpoint string
	logger   *zap.Logger
}

func NewHeartbeatClient(l *zap.Logger, endpoint string) *HeartbeatClient {
	l.Info("created client with endpoint: " + endpoint)
	return &HeartbeatClient{
		endpoint: endpoint,
		logger:   l.Named("heartbeat client"),
	}
}

func (c *HeartbeatClient) Heartbeat(ctx context.Context, req *HeartbeatRequest) (*HeartbeatResponse, error) {
	buf := bytes.Buffer{}
	if err := json.NewEncoder(&buf).Encode(req); err != nil {
		c.logger.Error(fmt.Sprintf("encoding request: %v", err))
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/heartbeat", &buf)
	if err != nil {
		c.logger.Error(fmt.Sprintf("creating http request: %v", err))
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpResp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		if !errors.Is(err, ctx.Err()) {
			c.logger.Error(fmt.Sprintf("getting response: %v", err))
		}
		return nil, err
	}
	defer func() { _ = httpResp.Body.Close() }()
	if httpResp.StatusCode != http.StatusOK {
		c.logger.Info(fmt.Sprintf("status error: %s", http.StatusText(httpResp.StatusCode)))
		errString, err := ioutil.ReadAll(httpResp.Body)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(string(errString))
	}
	var resp HeartbeatResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&resp); err != nil {
		c.logger.Error(fmt.Sprintf("decoding response: %v", err))
		return nil, err
	}
	return &resp, nil
}
