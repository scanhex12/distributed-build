//go:build !solution
// +build !solution

package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"gitlab.com/slon/shad-go/distbuild/pkg/build"
	"go.uber.org/zap"
)

func NewBuildService(l *zap.Logger, s Service) *BuildHandler {
	return &BuildHandler{
		logger:  l.Named("build handler"),
		service: s,
	}
}

type BuildHandler struct {
	logger  *zap.Logger
	service Service
}

type JSONStatusWriter struct {
	encoder *json.Encoder
	flush   func()
	logger  *zap.Logger
	opened  bool
	closed  bool
	mu      sync.Mutex
	done    chan struct{}
}

func (w *JSONStatusWriter) Started(rsp *BuildStarted) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.opened = true
	err := w.encoder.Encode(rsp)
	if err != nil {
		w.logger.Error(fmt.Sprintf("json encode: %v", err))
		return err
	}
	w.flush()
	return nil
}

func (w *JSONStatusWriter) Updated(update *StatusUpdate) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.opened = true
	err := w.encoder.Encode(update)
	if err != nil {
		w.logger.Error(fmt.Sprintf("json encode: %v", err))
		return err
	}
	w.flush()
	if update.BuildFinished != nil || update.BuildFailed != nil {
		if !w.closed {
			close(w.done)
			w.closed = true
		}
	}
	return nil
}

func (h *BuildHandler) startBuild(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()
	if r.Method != http.MethodPost {
		h.logger.Error(fmt.Sprintf("incorrect method"))
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	statusWriter := &JSONStatusWriter{
		encoder: json.NewEncoder(w),
		flush:   w.(http.Flusher).Flush,
		logger:  h.logger.Named("json status writer"),
		done:    make(chan struct{}),
	}
	var buildReq BuildRequest
	if err := json.NewDecoder(r.Body).Decode(&buildReq); err != nil {
		h.logger.Error(fmt.Sprintf("json error: %v", err))
		w.WriteHeader(http.StatusBadRequest)
		_ = statusWriter.Updated(&StatusUpdate{
			BuildFailed: &BuildFailed{err.Error()},
		})
		return
	}
	err := h.service.StartBuild(r.Context(), &buildReq, StatusWriter(statusWriter))
	if err != nil {
		statusWriter.mu.Lock()
		if !statusWriter.opened {
			w.WriteHeader(http.StatusInternalServerError)
		}
		statusWriter.mu.Unlock()
		_ = statusWriter.Updated(&StatusUpdate{
			BuildFailed: &BuildFailed{err.Error()},
		})
		return
	}
	<-statusWriter.done
}

func (h *BuildHandler) signalBuild(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()
	if r.Method != http.MethodPost {
		h.logger.Error(fmt.Sprintf("incorrect method"))
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var buildID build.ID
	err := buildID.UnmarshalText([]byte(r.URL.Query().Get("build_id")))
	if err != nil {
		h.logger.Error(fmt.Sprintf("decoding url: %v", err))
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	var signalReq SignalRequest
	if err = json.NewDecoder(r.Body).Decode(&signalReq); err != nil {
		h.logger.Error(fmt.Sprintf("json error: %v", err))
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	signalResp, err := h.service.SignalBuild(r.Context(), buildID, &signalReq)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err = json.NewEncoder(w).Encode(signalResp); err != nil {
		h.logger.Error(fmt.Sprintf("encode error: %v", err))
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
}

func (h *BuildHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/build", h.startBuild)
	mux.HandleFunc("/signal", h.signalBuild)
}
