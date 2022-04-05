package main

import (
	"context"
	"github.com/rsocket/rsocket-go"
	"github.com/rsocket/rsocket-go/payload"
	"github.com/rsocket/rsocket-go/rx"
	"sync"
)

func main() {
	client, err := rsocket.Connect().
		Transport(rsocket.TCPClient().SetHostAndPort("localhost", 9200).Build()).
		Start(context.Background())

	if err != nil {
		panic(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	client.RequestResponse(payload.NewString("mxq", "")).
		Subscribe(context.Background(), rx.OnNext(func(elem payload.Payload) error {
			println(elem.DataUTF8())
			return nil
		}), rx.OnComplete(func() {
			wg.Done()
		}))
	wg.Wait()
}
