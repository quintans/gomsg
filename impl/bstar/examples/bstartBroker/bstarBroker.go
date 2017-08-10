// THIS IS A WORK IN PROGRESS

package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/quintans/gomsg"
	"github.com/quintans/gomsg/impl/bstar"
	"github.com/quintans/toolkit/log"
)

func init() {
	gomsg.SetLogger(log.LoggerFor("github.com/quintans/gmsg"))
}

const PUB_PREFIX = "pub/"

func main() {
	handler := func(bstar *bstar.BStar, req *gomsg.Request) {
		// Consider any request under "pub/" to be a publish.
		// This is way we can publishing and receive a confirmation.
		// To allow for the other clients receive the a PUB message under the right topic
		// the kind and name are fixed.
		if strings.HasPrefix(req.Name, PUB_PREFIX) {
			req.Kind = gomsg.PUB
			req.Name = req.Name[len(PUB_PREFIX):]
		}
	}

	pAddr := "127.0.0.1:7011"
	bAddr := "127.0.0.1:7012"
	primary := bstar.NewBStar(true, "*:7001", "*:7011", bAddr)
	primary.SetClientHandler(handler)

	backup := bstar.NewBStar(false, "*:7002", "*:7012", pAddr)
	backup.SetClientHandler(handler)

	primary.Start()
	backup.Start()

	time.Sleep(time.Second)

	endpoint := fmt.Sprintf("%sTEST", PUB_PREFIX)
	h := func() {
		fmt.Println("I: server replied OK")
	}
	bsClient1 := bstar.NewBStarClient(pAddr, bAddr)
	bsClient1.Request(endpoint, "one two three", h, time.Second)

	time.Sleep(time.Second * 3)
}
