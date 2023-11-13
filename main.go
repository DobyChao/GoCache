package main

import (
	"flag"
	"fmt"
	"github.com/gin-gonic/gin"
	"gocache"
	"log"
	"net/http"
	"strings"
)

var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

func createGroup() *gocache.Group {
	return gocache.NewGroup("scores", 2<<10, gocache.GetterFunc(
		func(key string) ([]byte, error) {
			log.Println("[SlowDB] search key", key)
			if v, ok := db[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}))
}

func startCacheServer(addr string, addrs []string, g *gocache.Group) {
	peers := gocache.NewHTTPPool(addr)
	// set peers
	peers.Set(addrs...)
	g.RegisterPeers(peers)

	// start http server
	r := gin.Default()
	peers.LoadRouters(r)
	log.Println("gocache is running at", addr)
	addr = strings.TrimPrefix(addr, "http://")
	r.Run(addr)
}

func startAPIServer(apiAddr string, g *gocache.Group) {
	r := gin.Default()
	r.GET("/api", func(c *gin.Context) {
		key := c.Query("key")
		view, err := g.Get(key)
		if err != nil {
			c.String(http.StatusInternalServerError, err.Error())
			return
		}

		c.Data(http.StatusOK, "application/octet-stream", view.ByteSlice())
	})
	log.Println("fontend server is running at", apiAddr)
	apiAddr = strings.TrimPrefix(apiAddr, "http://")
	r.Run(apiAddr)
}

func main() {
	var (
		port int
		api  bool
	)
	// cli arguments
	flag.IntVar(&port, "port", 8001, "gocache server port") // which port to listen
	flag.BoolVar(&api, "api", false, "start a api server?")
	flag.Parse()

	apiAddr := "http://localhost:9999"

	addrMap := map[int]string{
		8001: "http://localhost:8001",
		8002: "http://localhost:8002",
		8003: "http://localhost:8003",
	}

	var addrs []string
	for _, v := range addrMap {
		addrs = append(addrs, v)
	}

	g := createGroup()
	if api {
		go startAPIServer(apiAddr, g)
	}
	startCacheServer(addrMap[port], []string(addrs), g)
}
