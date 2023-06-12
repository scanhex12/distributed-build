//go:build !solution
// +build !solution

package dist

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"gitlab.com/slon/shad-go/distbuild/pkg/build"

	"gitlab.com/slon/shad-go/distbuild/pkg/api"

	"go.uber.org/zap"

	"gitlab.com/slon/shad-go/distbuild/pkg/filecache"
	"gitlab.com/slon/shad-go/distbuild/pkg/scheduler"
)

type pendingBuild struct {
	graph build.Graph
	w     api.StatusWriter
}

type Coordinator struct {
	logger           *zap.Logger
	sched            *scheduler.Scheduler
	mux              *http.ServeMux
	heartbeatHandler *api.HeartbeatHandler
	buildHandler     *api.BuildHandler
	cache            *filecache.Cache
	fileCacheHandler *filecache.Handler
	workers          map[api.WorkerID]struct{}
	pendingBuilds    map[build.ID]pendingBuild
	mu               *sync.Mutex
}

var defaultConfig = scheduler.Config{
	CacheTimeout: time.Millisecond * 10,
	DepsTimeout:  time.Millisecond * 100,
}

func NewCoordinator(
	log *zap.Logger,
	fileCache *filecache.Cache,
) *Coordinator {
	c := &Coordinator{
		logger:           log,
		sched:            scheduler.NewScheduler(log, defaultConfig),
		mux:              http.NewServeMux(),
		cache:            fileCache,
		fileCacheHandler: filecache.NewHandler(log, fileCache),
		workers:          make(map[api.WorkerID]struct{}),
		pendingBuilds:    make(map[build.ID]pendingBuild),
		mu:               &sync.Mutex{},
	}
	c.heartbeatHandler = api.NewHeartbeatHandler(log, c)
	c.buildHandler = api.NewBuildService(log, c)
	c.heartbeatHandler.Register(c.mux)
	c.buildHandler.Register(c.mux)
	c.fileCacheHandler.Register(c.mux)
	return c
}

func (c *Coordinator) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.logger.Info(fmt.Sprintf("serving: %s", r.URL.Path))
	c.mux.ServeHTTP(w, r)
}

func (c *Coordinator) StartBuild(ctx context.Context, request *api.BuildRequest, w api.StatusWriter) error {
	c.logger.Info("start build")
	buildID := build.NewID()
	c.mu.Lock()
	_, ok := c.pendingBuilds[buildID]
	if ok {
		c.mu.Unlock()
		c.logger.Error("duplicate build id: this should be impossible")
		return errors.New("duplicate id")
	}
	c.pendingBuilds[buildID] = pendingBuild{
		graph: request.Graph,
		w:     w,
	}
	c.mu.Unlock()
	buildStarted := api.BuildStarted{
		ID:           buildID,
		MissingFiles: make([]build.ID, 0),
	}
	for id := range request.Graph.SourceFiles {
		_, unlock, err := c.cache.Get(id)
		if err == nil {
			unlock()
			continue
		}
		if !errors.Is(err, filecache.ErrNotFound) {
			return err
		}
		buildStarted.MissingFiles = append(buildStarted.MissingFiles, id)
	}
	c.logger.Info(fmt.Sprintf("start build response id: %v", buildID))
	err := w.Started(&buildStarted)
	return err
}

func (c *Coordinator) SignalBuild(ctx context.Context, buildID build.ID,
	signal *api.SignalRequest) (*api.SignalResponse, error) {
	c.logger.Info(fmt.Sprintf("signal build id: %v", buildID))
	if signal.UploadDone != nil {
		c.mu.Lock()
		pendingBuild := c.pendingBuilds[buildID]
		delete(c.pendingBuilds, buildID)
		c.mu.Unlock()
		jobs := build.TopSort(pendingBuild.graph.Jobs)
		graphSources := make(map[string]build.ID)
		for k, v := range pendingBuild.graph.SourceFiles {
			graphSources[v] = k
		}
		c.logger.Info("scheduling jobs")
		pendingResults := make(chan *scheduler.PendingJob)
		go func() {
			num := 0
			for pending := range pendingResults {
				num++
				update := &api.StatusUpdate{
					JobFinished:   pending.Result,
					BuildFailed:   nil,
					BuildFinished: nil,
				}
				if num == len(jobs) {
					update.BuildFinished = &api.BuildFinished{}
				}
				_ = pendingBuild.w.Updated(update)
				if num == len(jobs) {
					break
				}
			}
		}()
		for _, job := range jobs {
			select {
			case <-ctx.Done():
				return &api.SignalResponse{}, ctx.Err()
			default:
				break
			}
			jobSpec := &api.JobSpec{
				SourceFiles: make(map[build.ID]string),
				Artifacts:   nil,
				Job:         job,
			}
			for _, input := range job.Inputs {
				jobSpec.SourceFiles[graphSources[input]] = input
			}
			pending := c.sched.ScheduleJob(jobSpec)
			go func() {
				<-pending.Finished
				pendingResults <- pending
			}()
		}
		c.logger.Info("scheduling ended")
		return &api.SignalResponse{}, nil
	}
	return &api.SignalResponse{}, nil
}

func (c *Coordinator) Heartbeat(ctx context.Context, req *api.HeartbeatRequest) (*api.HeartbeatResponse, error) {
	c.logger.Info(fmt.Sprintf("heartbeat: %v", req.WorkerID))
	c.mu.Lock()
	_, ok := c.workers[req.WorkerID]
	if !ok {
		c.workers[req.WorkerID] = struct{}{}
	}
	c.mu.Unlock()
	if !ok {
		c.logger.Info("new worker")
		c.sched.RegisterWorker(req.WorkerID)
	}
	resp := api.HeartbeatResponse{
		JobsToRun: make(map[build.ID]api.JobSpec),
	}
	c.logger.Info(fmt.Sprintf("finished jobs num: %v", len(req.FinishedJob)))
	for i := range req.FinishedJob {
		c.sched.OnJobComplete(req.WorkerID, req.FinishedJob[i].ID, &req.FinishedJob[i])
	}
	// golang scheduler optimization: do picks in goroutines, because
	// scheduler will call them, if an event arrives to the chan
	// probably need WithDeadline
	var cancel context.CancelFunc
	mu := sync.Mutex{}
	var wg sync.WaitGroup
	wg.Add(req.FreeSlots)
	ctx, cancel = context.WithCancel(ctx)
	for i := 0; i < req.FreeSlots; i++ {
		go func() {
			defer wg.Done()
			pending := c.sched.PickJob(ctx, req.WorkerID)
			if pending == nil {
				return
			}
			mu.Lock()
			resp.JobsToRun[pending.Job.ID] = *pending.Job
			mu.Unlock()
		}()
	}
	cancel()
	wg.Wait()
	c.logger.Info(fmt.Sprintf("heartbeat response, len jobs: %v", len(resp.JobsToRun)))
	return &resp, nil
}
