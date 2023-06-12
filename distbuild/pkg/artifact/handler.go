//go:build !solution

package artifact

import (
	"gitlab.com/slon/shad-go/distbuild/pkg/build"
	"gitlab.com/slon/shad-go/distbuild/pkg/tarstream"
	"net/http"

	"go.uber.org/zap"
)

type Handler struct {
	logger *zap.Logger
	cache  *Cache
}

func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	var artifact build.ID
	err := artifact.UnmarshalText([]byte(request.URL.Query().Get("id")))
	if err != nil {
		return
	}
	path, unlock, err := h.cache.Get(artifact)
	defer unlock()
	err = tarstream.Send(path, writer)
	if err != nil {
		return
	}
}

func NewHandler(l *zap.Logger, c *Cache) *Handler {
	return &Handler{logger: l, cache: c}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.Handle("/artifact", h)
}
