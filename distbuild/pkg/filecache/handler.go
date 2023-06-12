//go:build !solution

package filecache

import (
	"gitlab.com/slon/shad-go/distbuild/pkg/build"
	"golang.org/x/sync/singleflight"
	"io"
	"net/http"
	"os"

	"go.uber.org/zap"
)

type Handler struct {
	logger *zap.Logger
	cache  *Cache
	group  singleflight.Group
}

func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method == http.MethodGet {
		h.GetRequest(writer, request)
	} else {
		h.PostRequest(writer, request)
	}
}

func (h *Handler) GetRequest(writer http.ResponseWriter, request *http.Request) {
	var id build.ID
	err := id.UnmarshalText([]byte(request.URL.Query().Get("id")))
	if err != nil {
		return
	}
	path, unlock, err := h.cache.Get(id)
	if err != nil {
		return
	}
	defer unlock()
	file, err := os.Open(path)
	if err != nil {
		return
	}
	_, err = io.Copy(writer, file)
	if err != nil {
		return
	}
}

func (h *Handler) PostRequest(writer http.ResponseWriter, request *http.Request) {
	var id build.ID
	err := id.UnmarshalText([]byte(request.URL.Query().Get("id")))
	if err != nil {
		return
	}
	_, err, _ = h.group.Do(id.String(), func() (interface{}, error) {
		i, erro := io.Copy(writer, request.Body)
		if erro != nil {
			return nil, erro
		}
		return i, erro
	})
	if err != nil {
		return
	}
}

func NewHandler(l *zap.Logger, cache *Cache) *Handler {
	return &Handler{
		logger: l,
		cache:  cache,
	}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.Handle("/file", h)
}
