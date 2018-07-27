package ui

import (
	"html/template"
	"net/http"

	"github.com/go-kit/kit/log"
)

const tmplIndex = `<!DOCTYPE html>
<html>
  <head profile="http://www.w3.org/2005/10/profile">
    <meta charset="utf-8">
    <title>OK Log</title>
    <link rel="icon" type="image/png" href="favicon.png">
    <link href="https://fonts.googleapis.com/css?family=Droid+Sans+Mono|Droid+Sans:400,700" rel="stylesheet">
    <link href="styles/normalize.css" rel="stylesheet">
    <link href="styles/store.css" rel="stylesheet">
    <script src="scripts/oklog.js" type="text/javascript"></script>
  </head>
  <body>
    <script src="scripts/ports.js" type="text/javascript"></script>
 </body>
</html>`

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
	var (
		mux     = http.NewServeMux()
		tplRoot = template.Must(template.New("root").Parse(tmplIndex))
	)

	mux.Handle("/scripts/", http.FileServer(_escFS(a.local)))
	mux.Handle("/styles/", http.FileServer(_escFS(a.local)))

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_ = tplRoot.Execute(w, nil)
	})

	mux.ServeHTTP(w, r)
}
