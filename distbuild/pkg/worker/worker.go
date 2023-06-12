//go:build !solution
// +build !solution

package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"gitlab.com/slon/shad-go/distbuild/pkg/build"
	"go.uber.org/zap"

	"gitlab.com/slon/shad-go/distbuild/pkg/api"
	"gitlab.com/slon/shad-go/distbuild/pkg/artifact"
	"gitlab.com/slon/shad-go/distbuild/pkg/filecache"
)

type State struct {
	mu             *sync.Mutex
	runningJobs    map[build.ID]struct{}
	freeSlots      int
	finishedJob    []api.JobResult
	addedArtifacts []build.ID
	jobCache       map[build.ID]*api.JobResult
}

const MaxWorkerFreeSlots = 15
const MinLoopDelay = 50 * time.Millisecond

type Worker struct {
	id                  api.WorkerID
	coordinatorEndpoint string
	logger              *zap.Logger
	fileCache           *filecache.Cache
	fileCacheClient     *filecache.Client
	artifacts           *artifact.Cache
	artifactHandler     *artifact.Handler
	heartbeatClient     *api.HeartbeatClient
	mux                 *http.ServeMux
	State
}

func New(
	workerID api.WorkerID,
	coordinatorEndpoint string,
	log *zap.Logger,
	fileCache *filecache.Cache,
	artifacts *artifact.Cache,
) *Worker {
	result := &Worker{
		id:                  workerID,
		coordinatorEndpoint: coordinatorEndpoint,
		logger:              log,
		fileCache:           fileCache,
		artifacts:           artifacts,
		mux:                 http.NewServeMux(),
		State: State{
			mu:          &sync.Mutex{},
			runningJobs: make(map[build.ID]struct{}),
			freeSlots:   MaxWorkerFreeSlots,
			jobCache:    make(map[build.ID]*api.JobResult),
		},
	}
	result.fileCacheClient = filecache.NewClient(result.logger, result.coordinatorEndpoint)
	result.artifactHandler = artifact.NewHandler(result.logger, result.artifacts)
	result.artifactHandler.Register(result.mux)
	result.heartbeatClient = api.NewHeartbeatClient(result.logger, result.coordinatorEndpoint)
	return result
}

func (w *Worker) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	w.mux.ServeHTTP(rw, r)
}

func (w *Worker) downloadFile(ctx context.Context, id build.ID) error {
	err := w.fileCacheClient.Download(ctx, w.fileCache, id)
	return err
}

func (w *Worker) downloadAcquireSingleFile(ctx context.Context, id build.ID,
	name string, newDir string) (unlock func(), err error) {
	var path string
	path, unlock, err = w.fileCache.Get(id)
	if errors.Is(err, filecache.ErrNotFound) {
		err = w.downloadFile(ctx, id)
		if err != nil {
			return
		}
		path, unlock, err = w.fileCache.Get(id)
		if err != nil {
			return
		}
	} else if err != nil {
		return
	}
	newPath := filepath.Join(newDir, name)
	err = os.Remove(newPath)
	if err != nil && !os.IsNotExist(err) {
		return
	}
	err = os.MkdirAll(filepath.Dir(newPath), artifact.DirectoryPerm)
	if err != nil {
		return
	}
	err = os.Link(path, newPath)
	return
}

func (w *Worker) downloadAcquireFiles(ctx context.Context,
	sourceFiles map[build.ID]string) (basePath string, globalUnlock func(), err error) {

	basePath, err = ioutil.TempDir("", "")
	if err != nil {
		return
	}
	var unlocks []func()
	globalUnlock = func() {
		for _, unlock := range unlocks {
			unlock()
		}
	}
	unlockChan := make(chan func())
	erChan := make(chan error)
	for id, name := range sourceFiles {
		curID := id
		curName := name
		go func() {
			unlock, downErr := w.downloadAcquireSingleFile(ctx, curID, curName, basePath)
			erChan <- downErr
			unlockChan <- unlock
		}()
	}
	for range sourceFiles {
		downErr := <-erChan
		if downErr != nil {
			err = downErr
		}
		unlock := <-unlockChan
		if unlock != nil {
			unlocks = append(unlocks, unlock)
		}
	}
	if err != nil {
		globalUnlock()
	}
	return
}

type artDownloadRes struct {
	id      build.ID
	path    string
	unlock  func()
	isAdded bool
	err     error
}

func (w *Worker) downloadAcquireSingleArtifact(ctx context.Context, id build.ID,
	workerID api.WorkerID) (res artDownloadRes) {
	res.id = id
	res.path, res.unlock, res.err = w.artifacts.Get(id)
	if res.err != nil {
		if !errors.Is(res.err, artifact.ErrNotFound) {
			return
		}
		res.err = artifact.Download(ctx, workerID.String(), w.artifacts, id)
		if res.err != nil {
			return
		}
		res.path, res.unlock, res.err = w.artifacts.Get(id)
		if res.err != nil {
			return
		}
		res.isAdded = true
	}
	return
}

func (w *Worker) downloadAcquireArtifacts(ctx context.Context,
	artifcats map[build.ID]api.WorkerID) (paths map[build.ID]string, added []build.ID,
	globalUnlock func(), err error) {

	paths = make(map[build.ID]string)
	var unlocks []func()
	globalUnlock = func() {
		for _, unlock := range unlocks {
			unlock()
		}
	}

	downloadChan := make(chan artDownloadRes)
	for id, workerID := range artifcats {
		curID := id
		curWorker := workerID
		go func() {
			downloadChan <- w.downloadAcquireSingleArtifact(ctx, curID, curWorker)
		}()
	}
	for range artifcats {
		res := <-downloadChan
		if res.err == nil {
			paths[res.id] = res.path
			if res.isAdded {
				added = append(added, res.id)
			}
			if res.unlock != nil {
				unlocks = append(unlocks, res.unlock)
			}
		} else {
			err = res.err
		}
	}
	if err != nil {
		globalUnlock()
	}
	return
}

func executeCommandContext(ctx context.Context,
	cmd *build.Cmd, stdOut io.Writer, stdErr io.Writer) (code int, err error) {

	if cmd.Exec != nil && len(cmd.Exec) > 0 {
		execCommand := exec.CommandContext(ctx, cmd.Exec[0], cmd.Exec[1:]...)
		execCommand.Dir = cmd.WorkingDirectory
		execCommand.Env = cmd.Environ
		execCommand.Stdout = stdOut
		execCommand.Stderr = stdErr
		err = execCommand.Run()
		code = execCommand.ProcessState.ExitCode()
	} else if len(cmd.CatOutput) > 0 {
		var f *os.File
		f, err = os.Create(cmd.CatOutput)
		if err == nil {
			_, err = f.WriteString(cmd.CatTemplate)
		}
	}
	return
}

func (w *Worker) tryTakeJobFromCache(jobID build.ID) bool {
	w.State.mu.Lock()
	defer w.State.mu.Unlock()
	jobResult, ok := w.State.jobCache[jobID]
	if ok {
		w.State.freeSlots++
		w.State.finishedJob = append(w.State.finishedJob, *jobResult)
		delete(w.State.runningJobs, jobID)
		w.logger.Info("loaded cached job")
	}
	return ok
}

func (w *Worker) runJob(ctx context.Context, jobID build.ID, jobSpec api.JobSpec) {
	if w.tryTakeJobFromCache(jobID) {
		return
	}
	w.logger.Info(fmt.Sprintf("processing job: %v", jobID))
	sources, sourceUnlock, err := w.downloadAcquireFiles(ctx, jobSpec.SourceFiles)
	if err != nil {
		panic(err)
	}
	w.logger.Info(fmt.Sprintf("download artifacts, should be %v", len(jobSpec.Artifacts)))
	artifacts, added, artUnlock, err := w.downloadAcquireArtifacts(ctx, jobSpec.Artifacts)
	w.logger.Info(fmt.Sprintf("end download: len %v, added %v", len(artifacts), len(added)))
	if err != nil {
		panic(err)
	}
	w.State.mu.Lock()
	w.State.addedArtifacts = append(w.State.addedArtifacts, added...)
	w.State.mu.Unlock()
	outPath, commit, abort, err := w.artifacts.Create(jobID)
	if err != nil {
		panic(err)
	}
	jobContext := build.JobContext{
		SourceDir: sources,
		OutputDir: outPath,
		Deps:      artifacts,
	}
	stdOut := bytes.Buffer{}
	stdErr := bytes.Buffer{}
	code := 0
	w.logger.Info("job executing cmd")
	for _, cmd := range jobSpec.Cmds {
		var newCmd *build.Cmd
		newCmd, err = cmd.Render(jobContext)
		if err != nil {
			panic(err)
		}
		code, err = executeCommandContext(ctx, newCmd, &stdOut, &stdErr)
		if err != nil {
			break
		}
	}
	w.logger.Info("finish execution")
	sourceUnlock()
	artUnlock()

	jobResult := &api.JobResult{
		ID:       jobID,
		Stdout:   stdOut.Bytes(),
		Stderr:   stdErr.Bytes(),
		ExitCode: code,
		Error:    nil,
	}
	if err != nil {
		errString := err.Error()
		jobResult.Error = &errString
		if abortErr := abort(); abortErr != nil {
			panic(abortErr)
		}
	} else {
		if err = commit(); err != nil {
			panic(err)
		}
	}

	w.State.mu.Lock()
	w.State.freeSlots++
	w.State.finishedJob = append(w.State.finishedJob, *jobResult)
	if err == nil {
		w.State.addedArtifacts = append(w.State.addedArtifacts, jobID)
		w.State.jobCache[jobID] = jobResult
	}
	delete(w.State.runningJobs, jobID)
	w.State.mu.Unlock()
	w.logger.Info("ended job")
}

func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("starting worker")
	for {
		w.logger.Info("tick")
		select {
		case <-time.After(MinLoopDelay):
			break
		case <-ctx.Done():
			return ctx.Err()
		}
		w.mu.Lock()
		req := api.HeartbeatRequest{
			WorkerID:       w.id,
			RunningJobs:    make([]build.ID, len(w.runningJobs)),
			FreeSlots:      w.freeSlots,
			FinishedJob:    w.finishedJob,
			AddedArtifacts: w.addedArtifacts,
		}
		i := 0
		for k := range w.runningJobs {
			req.RunningJobs[i] = k
			i++
		}
		w.finishedJob = nil
		w.addedArtifacts = nil
		w.mu.Unlock()
		w.logger.Info("sending heartbeat")
		resp, err := w.heartbeatClient.Heartbeat(ctx, &req)
		if err != nil {
			return err
		}
		w.logger.Info("got heartbeat")
		w.mu.Lock()
		w.State.freeSlots -= len(resp.JobsToRun)
		for jobID := range resp.JobsToRun {
			w.runningJobs[jobID] = struct{}{}
		}
		w.mu.Unlock()
		for jobID, jobSpec := range resp.JobsToRun {
			curJobID := jobID
			curJobSpec := jobSpec
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				go w.runJob(ctx, curJobID, curJobSpec)
			}
		}
	}
}
