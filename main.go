package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"gocache"
	"log"
)

var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

func main() {
	gocache.NewGroup("scores", 2<<10, gocache.GetterFunc(
		func(key string) ([]byte, error) {
			log.Println("[SlowDB] search key", key)
			if v, ok := db[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}))

	r := gin.Default()
	peers := gocache.NewHTTPPool("localhost:9999")
	peers.LoadRouters(r)
	r.Run(":9999")
}
