//go:build !solution
// +build !solution

package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"go.uber.org/zap"
)

type HeartbeatHandler struct {
	logger  *zap.Logger
	service HeartbeatService
}

func NewHeartbeatHandler(l *zap.Logger, s HeartbeatService) *HeartbeatHandler {
	return &HeartbeatHandler{
		logger:  l.Named("heartbeat handler"),
		service: s,
	}
}

func (h *HeartbeatHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()
	if r.Method != http.MethodPost {
		h.logger.Error(fmt.Sprintf("incorrect method"))
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var hbReq HeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&hbReq); err != nil {
		h.logger.Error(fmt.Sprintf("json error: %v", err))
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	h.logger.Info("go to service")
	hbResp, err := h.service.Heartbeat(r.Context(), &hbReq)
	h.logger.Info("return from service")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		h.logger.Info("status error")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(hbResp); err != nil {
		h.logger.Error(fmt.Sprintf("encode hb error: %v", err))
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	h.logger.Info("end handler")
}

func (h *HeartbeatHandler) Register(mux *http.ServeMux) {
	mux.Handle("/heartbeat", h)
}
