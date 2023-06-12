//go:build !solution

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"gitlab.com/slon/shad-go/distbuild/pkg/build"
	"go.uber.org/zap"
	"io"
	"net/http"
	"net/url"
)

type ClientStatusReader struct {
	decoder *json.Decoder
	reader  io.ReadCloser
}

func (c *ClientStatusReader) Next() (*StatusUpdate, error) {
	var status StatusUpdate
	err := c.decoder.Decode(&status)
	if err != nil {
		return nil, err
	}
	return &status, nil
}

func (c *ClientStatusReader) Close() error {
	err := c.reader.Close()
	return err
}

func NewClientStatusReader(decoder *json.Decoder, reader io.ReadCloser) *ClientStatusReader {
	statusReader := ClientStatusReader{
		reader:  reader,
		decoder: decoder,
	}
	return &statusReader
}

type BuildClient struct {
	logger   *zap.Logger
	endpoint string
}

func NewBuildClient(l *zap.Logger, endpoint string) *BuildClient {
	client := BuildClient{logger: l, endpoint: endpoint}
	return &client
}

func (c *BuildClient) StartBuild(ctx context.Context, request *BuildRequest) (*BuildStarted, StatusReader, error) {
	buffer := bytes.Buffer{}
	encoder := json.NewEncoder(&buffer)
	err := encoder.Encode(request)
	if err != nil {
		return nil, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/build", &buffer)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer func(Body io.ReadCloser) {
			err := Body.Close()
			if err != nil {

			}
		}(resp.Body)
		return nil, nil, errors.New("")
	}
	decoder := json.NewDecoder(resp.Body)
	var buildStarted BuildStarted
	err = decoder.Decode(&buildStarted)
	if err != nil {
		return nil, nil, err
	}
	statusReader := NewClientStatusReader(decoder, resp.Body)
	return &buildStarted, statusReader, nil
}

func (c *BuildClient) SignalBuild(ctx context.Context, buildID build.ID, signal *SignalRequest) (*SignalResponse, error) {
	buffer := bytes.Buffer{}
	encoder := json.NewEncoder(&buffer)
	err := encoder.Encode(signal)
	if err != nil {
		return nil, err
	}
	urlAdress, err := url.Parse(c.endpoint + "/signal")
	query := urlAdress.Query()
	query.Set("build_id", buildID.String())
	urlAdress.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, urlAdress.String(), &buffer)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	var signalResp SignalResponse
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("")
	}
	err = json.NewDecoder(resp.Body).Decode(&signalResp)
	if err != nil {
		return nil, err
	}
	return &signalResp, nil
}
