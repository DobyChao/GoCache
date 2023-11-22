package gocache

import (
	"fmt"
	"github.com/gin-gonic/gin"
	pb "gocache/cachepb"
	"gocache/consistenthash"
	"google.golang.org/protobuf/proto"
	"io"
	"log"
	"net/http"
	"net/url"
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
	router.GET("/", p.handleCheckEnabled)
	router.POST("/set-peers", p.handleSetPeers)
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

	// Write the value to the response body as a proto message.
	body, err := proto.Marshal(&pb.Response{Value: view.ByteSlice()})
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	c.Data(http.StatusOK, "application/octet-stream", body)
}

func (p *HTTPPool) handleCheckEnabled(c *gin.Context) {
	c.String(http.StatusOK, "ok")
}

func (p *HTTPPool) handleSetPeers(c *gin.Context) {
	var peers []string
	if err := c.ShouldBindJSON(&peers); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	p.Set(peers...)
	c.String(http.StatusOK, "ok")
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

func (h *httpGetter) Get(in *pb.Request, out *pb.Response) error {
	u := fmt.Sprintf(
		"%v%v/%v",
		h.baseURL,
		url.QueryEscape(in.GetGroup()),
		url.QueryEscape(in.GetKey()),
	)
	log.Println("httpGetter url:", u)
	res, err := http.Get(u)
	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned: %v", res.Status)
	}

	bytes, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %v", err)
	}

	if err := proto.Unmarshal(bytes, out); err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}

	return nil
}

// check that httpGetter implements PeerGetter
var _ PeerGetter = (*httpGetter)(nil)
