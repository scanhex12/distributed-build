//go:build !solution
// +build !solution

package scheduler

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"go.uber.org/zap"

	"gitlab.com/slon/shad-go/distbuild/pkg/api"
	"gitlab.com/slon/shad-go/distbuild/pkg/build"
)

var timeAfter = time.After

type PendingJob struct {
	Job      *api.JobSpec
	Finished chan struct{}
	Result   *api.JobResult
}

type Config struct {
	CacheTimeout time.Duration
	DepsTimeout  time.Duration
}

type workerSpec struct {
	id              api.WorkerID
	readyJobs       chan chan *PendingJob
	almostReadyJobs chan chan *PendingJob
}

type waitObject struct {
	closed   bool
	done     chan struct{}
	workerID api.WorkerID
	jobID    build.ID
}

const GlobalJobsChanSize = 300

type Scheduler struct {
	logger          *zap.Logger
	config          Config
	workers         map[api.WorkerID]workerSpec
	artifactWorkers map[build.ID][]api.WorkerID
	jobs            chan chan *PendingJob
	mu              sync.RWMutex
	fullJobs        map[build.ID]*PendingJob
	scheduled       map[build.ID]*waitObject
}

func NewScheduler(l *zap.Logger, config Config) *Scheduler {
	return &Scheduler{
		logger:          l.Named("scheduler"),
		config:          config,
		workers:         make(map[api.WorkerID]workerSpec),
		artifactWorkers: make(map[build.ID][]api.WorkerID),
		jobs:            make(chan chan *PendingJob, GlobalJobsChanSize),
		fullJobs:        make(map[build.ID]*PendingJob),
		scheduled:       make(map[build.ID]*waitObject),
	}
}

func (c *Scheduler) LocateArtifact(id build.ID) (api.WorkerID, bool) {
	canWorkers, ok := c.artifactWorkers[id]
	if !ok || len(canWorkers) == 0 {
		return "", false
	}
	return canWorkers[rand.Intn(len(canWorkers))], true
}

func (c *Scheduler) RegisterWorker(workerID api.WorkerID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workers[workerID] = workerSpec{
		id:              workerID,
		readyJobs:       make(chan chan *PendingJob),
		almostReadyJobs: make(chan chan *PendingJob),
	}
}

func (c *Scheduler) OnJobComplete(workerID api.WorkerID, jobID build.ID, res *api.JobResult) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	pending, ok := c.fullJobs[jobID]
	if ok {
		c.scheduled[jobID].workerID = workerID
		pending.Result = res
		delete(c.fullJobs, jobID)
	} else {
		c.scheduled[jobID] = &waitObject{
			closed:   true,
			done:     make(chan struct{}),
			workerID: workerID,
			jobID:    jobID,
		}
	}
	workers := c.artifactWorkers[jobID]
	c.artifactWorkers[jobID] = append(workers, workerID)
	waitObj := c.scheduled[jobID]
	if !waitObj.closed {
		waitObj.closed = true
		close(waitObj.done)
	}
	if ok {
		close(pending.Finished)
	}
	return ok
}

func (c *Scheduler) ScheduleJob(job *api.JobSpec) *PendingJob {
	c.mu.Lock()
	defer c.mu.Unlock()
	pending, ok := c.fullJobs[job.ID]
	if ok {
		return pending
	}
	job.Artifacts = make(map[build.ID]api.WorkerID)
	var badArts []*waitObject
	for _, dep := range job.Deps {
		id, found := c.LocateArtifact(dep)
		if found {
			job.Artifacts[dep] = id
		} else {
			badArts = append(badArts, c.scheduled[dep])
		}
	}
	_, ok = c.scheduled[job.ID]
	if !ok {
		c.scheduled[job.ID] = &waitObject{
			closed:   false,
			done:     make(chan struct{}),
			workerID: "",
			jobID:    job.ID,
		}
	}
	pending = &PendingJob{
		Job:      job,
		Finished: make(chan struct{}),
		Result:   nil,
	}
	c.fullJobs[job.ID] = pending
	go func() {
		i := 0
		for len(job.Artifacts) < len(job.Deps) {
			<-badArts[i].done

			job.Artifacts[badArts[i].jobID] = badArts[i].workerID
			i++
		}

		myChan := make(chan *PendingJob, 1)
		myChan <- pending
		given := false
		c.mu.RLock()
		for _, workerID := range c.artifactWorkers[job.ID] {
			given = true
			spec := c.workers[workerID]
			go func() {
				spec.readyJobs <- myChan
			}()
		}
		c.mu.RUnlock()
		if given {
			<-timeAfter(c.config.CacheTimeout)
		}
		pendingPtr := <-myChan
		if pendingPtr == nil {
			return
		}
		myChan <- pending
		workers := make(map[api.WorkerID]struct{})
		c.mu.RLock()
		for _, dep := range job.Deps {
			for _, workerID := range c.artifactWorkers[dep] {
				_, ok := workers[workerID]
				if !ok {
					workers[workerID] = struct{}{}
					spec := c.workers[workerID]
					go func() {
						spec.almostReadyJobs <- myChan
					}()
				}
			}
		}
		c.mu.RUnlock()
		<-timeAfter(c.config.DepsTimeout)
		pendingPtr = <-myChan
		if pendingPtr == nil {
			return
		}
		myChan <- pending
		go func() {
			c.jobs <- myChan
		}()
	}()
	return pending
}

func readPending(pendChan chan *PendingJob) *PendingJob {
	pending := <-pendChan
	if pending != nil {
		close(pendChan)
	}
	return pending
}

func (c *Scheduler) PickJob(ctx context.Context, workerID api.WorkerID) (pending *PendingJob) {
	c.mu.RLock()
	spec, ok := c.workers[workerID]
	c.mu.RUnlock()
	if !ok {
		return nil
	}
loop:
	for {
		select {
		case v := <-spec.readyJobs:
			pending = readPending(v)
		case v := <-spec.almostReadyJobs:
			pending = readPending(v)
		case v := <-c.jobs:
			pending = readPending(v)
		default:
			break loop
		}
		if pending != nil {
			return
		}
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case v := <-spec.readyJobs:
			pending = readPending(v)
		case v := <-spec.almostReadyJobs:
			pending = readPending(v)
		case v := <-c.jobs:
			pending = readPending(v)
		}
		if pending != nil {
			return
		}
	}
}
