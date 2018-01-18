package ui

import (
	"net/http"

	"github.com/go-kit/kit/log"
)

// API serves the ui API.
type API struct {
	local  bool
	logger log.Logger
}

// NewAPI returns a usable API.
func NewAPI(logger log.Logger, local bool) *API {
	return &API{
		local:  local,
		logger: logger,
	}
}

func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	http.FileServer(_escFS(a.local)).ServeHTTP(w, r)
}
