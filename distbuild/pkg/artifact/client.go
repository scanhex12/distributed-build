//go:build !solution

package artifact

import (
	"context"
	"gitlab.com/slon/shad-go/distbuild/pkg/build"
	"gitlab.com/slon/shad-go/distbuild/pkg/tarstream"
	"net/http"
	"net/url"
)

// Download artifact from remote cache into local cache.
func Download(ctx context.Context, endpoint string, c *Cache, artifactID build.ID) error {
	/*
		requestWithArgs := url.Values{}
		requestWithArgs.Add("id", artifactID.String())
		u, _ := url.ParseRequestURI(endpoint)
		u.RawQuery = requestWithArgs.Encode()
	*/
	uri, _ := url.Parse(endpoint + "/artifact")
	query := uri.Query()
	query.Set("id", artifactID.String())
	uri.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, uri.String(), nil)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	path, commit, abort, err := c.Create(artifactID)
	if err != nil {
		return err
	}
	err = tarstream.Receive(path, response.Body)
	if err != nil {
		err = abort()
		if err != nil {
			return err
		}
		return err
	}
	return commit()

}
