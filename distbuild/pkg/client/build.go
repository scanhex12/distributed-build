//go:build !solution
// +build !solution

package client

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"gitlab.com/slon/shad-go/distbuild/pkg/filecache"

	"gitlab.com/slon/shad-go/distbuild/pkg/api"

	"go.uber.org/zap"

	"gitlab.com/slon/shad-go/distbuild/pkg/build"
)

type Client struct {
	logger          *zap.Logger
	apiEndpoint     string
	sourceDir       string
	buildClient     *api.BuildClient
	fileCacheClient *filecache.Client
}

func NewClient(
	l *zap.Logger,
	apiEndpoint string,
	sourceDir string,
) *Client {
	return &Client{
		logger:          l,
		apiEndpoint:     apiEndpoint,
		sourceDir:       sourceDir,
		buildClient:     api.NewBuildClient(l, apiEndpoint),
		fileCacheClient: filecache.NewClient(l, apiEndpoint),
	}
}

type BuildListener interface {
	OnJobStdout(jobID build.ID, stdout []byte) error
	OnJobStderr(jobID build.ID, stderr []byte) error

	OnJobFinished(jobID build.ID) error
	OnJobFailed(jobID build.ID, code int, error string) error
}

func notifyListener(jobResult *api.JobResult, lsn BuildListener) (err error) {
	if jobResult.Error != nil {
		err = lsn.OnJobFailed(jobResult.ID, jobResult.ExitCode, *jobResult.Error)
	} else {
		err = lsn.OnJobFinished(jobResult.ID)
	}
	if err != nil {
		return
	}
	if err = lsn.OnJobStdout(jobResult.ID, jobResult.Stdout); err != nil {
		return
	}
	err = lsn.OnJobStderr(jobResult.ID, jobResult.Stderr)
	return
}

func (c *Client) Build(ctx context.Context, graph build.Graph, lsn BuildListener) (err error) {
	c.logger.Info("starting build")
	request := &api.BuildRequest{Graph: graph}
	buildStarted, reader, err := c.buildClient.StartBuild(ctx, request)
	if err != nil {
		return
	}
	defer func() { _ = reader.Close() }()
	erChan := make(chan error)
	c.logger.Info("uploading missing files")
	for _, missingID := range buildStarted.MissingFiles {
		curID := missingID
		path := graph.SourceFiles[missingID]
		go func() {
			upErr := c.fileCacheClient.Upload(ctx, curID, filepath.Join(c.sourceDir, path))
			erChan <- upErr
		}()
	}
	for range buildStarted.MissingFiles {
		val := <-erChan
		if val != nil {
			err = val
		}
	}
	if err != nil {
		return
	}
	c.logger.Info("send upload done")
	_, err = c.buildClient.SignalBuild(ctx, buildStarted.ID,
		&api.SignalRequest{UploadDone: new(api.UploadDone)})
	if err != nil {
		return
	}
	c.logger.Info("start receive updates")
loop:
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			break loop
		default:
			break
		}
		var update *api.StatusUpdate
		update, err = reader.Next()
		c.logger.Info("received update")
		if err != nil {
			break
		}
		if update.JobFinished != nil {
			err = notifyListener(update.JobFinished, lsn)
			if err != nil {
				break
			}
		}
		if update.BuildFailed != nil {
			err = errors.New(update.BuildFailed.Error)
			break
		}
		if update.BuildFinished != nil {
			break
		}
	}
	c.logger.Info(fmt.Sprintf("ended build with error: %v", err))
	return
}
