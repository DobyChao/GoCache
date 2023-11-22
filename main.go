package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"gocache"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
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
			time.Sleep(150 * time.Millisecond) // simulate slow database
			if v, ok := db[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}))
}

func startCacheServer(addr string, g *gocache.Group) {
	peers := gocache.NewHTTPPool(addr)
	// set peers
	peers.Set(addr)
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

func isSameSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func startMgrServer(allAddrs []string) {
	log.Println("Start manager server")
	// 每隔 5s 轮询一次，更新节点信息
	addrs := []string{}
	for {
		log.Println("Check addrs")
		done := make(chan bool)
		go func(ch chan bool) {
			availableAddrs := getAvailableAddrs(allAddrs)
			if !isSameSlice(availableAddrs, addrs) {
				addrs = availableAddrs
				log.Println("Update addrs", addrs)
				updateNodeInfo(addrs)
			}
			ch <- true
		}(done)
		time.Sleep(5 * time.Second)
		<-done
	}
}

func updateNodeInfo(peers []string) {
	for _, peer := range peers {
		jsonData, err := json.Marshal(peers)
		if err != nil {
			log.Println(err)
			continue
		}
		_, err = http.Post(peer+"/set-peers", "application/json", strings.NewReader(string(jsonData)))
		if err != nil {
			log.Println("Update node info error: ", err)
		}
	}
}

func getAvailableAddrs(addrs []string) []string {
	var availableAddrs []string
	for _, addr := range addrs {
		if isAddrAvailable(addr) {
			log.Println("Addr is available: ", addr)
			availableAddrs = append(availableAddrs, addr)
		}
	}
	return availableAddrs
}

func isAddrAvailable(addr string) bool {
	// 发送 HTTP 请求检查地址是否可访问
	resp, err := http.Get(addr)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func main() {
	var (
		port int
		api  bool
		mgr  bool
	)
	// cli arguments
	flag.IntVar(&port, "port", 8001, "gocache server port") // which port to listen
	flag.BoolVar(&api, "api", false, "start a api server?")
	flag.BoolVar(&mgr, "mgr", false, "start a manager server?")
	flag.Parse()

	if !mgr {
		addr := fmt.Sprintf("http://localhost:%d", port)
		g := createGroup()
		if api {
			apiAddr := "http://localhost:9999"
			go startAPIServer(apiAddr, g)
		}
		startCacheServer(addr, g)
	} else {
		addrs := []string{
			"http://localhost:8001",
			"http://localhost:8002",
			"http://localhost:8003",
		}
		startMgrServer(addrs)
	}
}

func init() {
	gin.SetMode(gin.ReleaseMode)
}
