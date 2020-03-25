// Copyright 2020 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package ethflare

import (
	"net/http"
	"path"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
)

// ServerConfig is a set of options used to config ethflare server.
type ServerConfig struct {
	Path      string // The file path points to database, empty means memory db
	RateLimit uint64 // The rate limit config for each backend
}

// Server is the frontend to accept les requests, relay them to proper
// backend and return the response.
type Server struct {
	config *ServerConfig

	// tiler is responible for indexing state trie and providing state
	// request validation service.
	tiler *tiler

	lock     sync.RWMutex
	backends *backendSet
}

func NewServer(config *ServerConfig) (*Server, error) {
	db := rawdb.NewMemoryDatabase()
	if config.Path != "" {
		ldb, err := rawdb.NewLevelDBDatabase(config.Path, 1024, 512, "tile")
		if err != nil {
			return nil, err
		}
		db = ldb
	}
	backends := newBackendSet()
	return &Server{
		config:   config,
		backends: backends,
		tiler:    newTiler(db, backends.removeBackend),
	}, nil
}

func (s *Server) Start(address string) {
	s.tiler.start()
	go http.ListenAndServe(address, newGzipHandler(s))

	log.Info("Ethflare started", "listen", address)
}

func (s *Server) Stop() {
	s.tiler.stop()
	log.Info("Ethflare stopped")
}

func (s *Server) RegisterBackends(ids []string, clients []*rpc.Client) {
	if len(ids) != len(clients) {
		return
	}
	var backends []*backend
	for index, client := range clients {
		backend, err := newBackend(ids[index], client, s.config.RateLimit)
		if err != nil {
			log.Debug("Failed to initialize backend", "error", err)
			continue
		}
		backend.start()
		err = s.backends.addBackend(ids[index], backend)
		if err != nil {
			log.Debug("Failed to register backend", "error", err)
			continue
		}
		backends = append(backends, backend)
	}
	// Notify other components for new backends arrival
	for _, backend := range backends {
		s.tiler.registerBackend(backend)
	}
}

// ServeHTTP is the entry point of the les cdn, splitting the request across the
// supported submodules.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch shift(&r.URL.Path) {
	case "chain":
		s.serveChain(w, r)
		return
	}
	http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
}

// shift splits off the first component of p, which will be cleaned of relative
// components before processing. The returned head will never contain a slash and
// the remaining tail will always be a rooted path without trailing slash.
func shift(p *string) string {
	*p = path.Clean("/" + *p)

	var head string
	if idx := strings.Index((*p)[1:], "/") + 1; idx > 0 {
		head = (*p)[1:idx]
		*p = (*p)[idx:]
	} else {
		head = (*p)[1:]
		*p = "/"
	}
	return head
}

// replyAndCache marshals a value into the response stream via RLP, also setting caching
// to indefinite.
func replyAndCache(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Cache-Control", "max-age=31536000") // 1 year cache expiry
	if err := rlp.Encode(w, v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// reply marshals a value into the response stream via RLP but not caches it.
func reply(w http.ResponseWriter, v interface{}) {
	if err := rlp.Encode(w, v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
