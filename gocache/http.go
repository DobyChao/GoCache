package gocache

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"gocache/consistenthash"
	"io"
	"log"
	"net/http"
	"sync"
)

const (
	defaultBasePath = "/_gocache/"
	defaultReplicas = 50
)

// HTTPPool implements PeerPicker for a pool of HTTP peers.
type HTTPPool struct {
	self        string                 // e.g. "localhost:8000"
	basePath    string                 // e.g. "/_gocache/"
	mu          sync.Mutex             // guards peers and httpGetters
	peers       *consistenthash.Map    // a map of peers
	httpGetters map[string]*httpGetter // keyed by e.g. "http://10.0.0.2:8008"
}

// NewHTTPPool initializes an HTTP pool of peers.
func NewHTTPPool(self string) *HTTPPool {
	return &HTTPPool{
		self:     self,
		basePath: defaultBasePath,
	}
}

func (p *HTTPPool) LoadRouters(router *gin.Engine) {
	router.GET(p.basePath+"/:groupname/:key", p.handleGetCache)
}

func (p *HTTPPool) handleGetCache(c *gin.Context) {
	groupname := c.Param("groupname")
	key := c.Param("key")

	group := GetGroup(groupname)
	if group == nil {
		c.String(http.StatusBadRequest, "no such group")
		return
	}

	view, err := group.Get(key)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	c.Data(http.StatusOK, "application/octet-stream", view.ByteSlice())
}

// Set update the pool's list of peers.
func (p *HTTPPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers = consistenthash.New(defaultReplicas, nil)
	p.peers.Add(peers...)
	p.httpGetters = make(map[string]*httpGetter, len(peers))
	for _, peer := range peers {
		p.httpGetters[peer] = &httpGetter{baseURL: peer + p.basePath}
	}
}

// PickPeer picks a peer according to key.
func (p *HTTPPool) PickPeer(key string) (PeerGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if peer := p.peers.Get(key); peer != "" && peer != p.self {
		log.Printf("Pick peer %s", peer)
		return p.httpGetters[peer], true
	}

	return nil, false
}

// check that HTTPPool implements PeerPicker
var _ PeerPicker = (*HTTPPool)(nil)

type httpGetter struct {
	baseURL string
}

func (h *httpGetter) Get(group string, key string) ([]byte, error) {
	url := h.baseURL + "/" + group + "/" + key
	res, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned: %v", res.Status)
	}

	bytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %v", err)
	}

	return bytes, nil
}

// check that httpGetter implements PeerGetter
var _ PeerGetter = (*httpGetter)(nil)
