//go:build !solution
// +build !solution

package filecache

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"

	"go.uber.org/zap"

	"gitlab.com/slon/shad-go/distbuild/pkg/build"
)

type Client struct {
	logger   *zap.Logger
	endpoint string
}

func NewClient(l *zap.Logger, endpoint string) *Client {
	return &Client{
		logger:   l.Named("filecache client"),
		endpoint: endpoint,
	}
}

func (c *Client) Upload(ctx context.Context, id build.ID, localPath string) error {
	uri, err := url.Parse(c.endpoint + "/file")
	if err != nil {
		return err
	}
	q := uri.Query()
	q.Set("id", id.String())
	uri.RawQuery = q.Encode()
	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, uri.String(), f)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		var body []byte
		body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return errors.New(string(body))
	}
	return nil
}

func (c *Client) Download(ctx context.Context, localCache *Cache, id build.ID) error {
	w, abort, err := localCache.Write(id)
	defer func() { _ = w.Close() }()
	if err != nil {
		return err
	}
	uri, err := url.Parse(c.endpoint + "/file")
	if err != nil {
		return err
	}
	q := uri.Query()
	q.Set("id", id.String())
	uri.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri.String(), nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		if abortErr := abort(); abortErr != nil {
			return abortErr
		}
		var body []byte
		body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return errors.New(string(body))
	}
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		if abortErr := abort(); abortErr != nil {
			return abortErr
		}
		return err
	}
	return nil
}
