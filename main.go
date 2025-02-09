package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	url := os.Getenv("NATS_SERVICE_NAME")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, _ := nats.Connect(url)
	defer func(nc *nats.Conn) {
		err := nc.Drain()
		if err != nil {

		}
	}(nc)

	js, _ := jetstream.New(nc)

	// JetStream API uses context for timeouts and cancellation
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// a key-value bucket is created in NATS
	kv, _ := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "profiles",
	})

	put, err := kv.Put(ctx, "sue.color", []byte("blue"))
	fmt.Println(put)
	if err != nil {
		return
	}
	entry, _ := kv.Get(ctx, "sue.color")

	// revision number of the entries will always be tracked
	fmt.Printf("%s @ %d -> %q\n", entry.Key(), entry.Revision(), string(entry.Value()))

	put, err = kv.Put(ctx, "sue.color", []byte("green"))
	fmt.Println(put)
	if err != nil {
		return
	}

	entry, _ = kv.Get(ctx, "sue.color")
	fmt.Printf("%s @ %d -> %q\n", entry.Key(), entry.Revision(), string(entry.Value()))

	_, err = kv.Update(ctx, "sue.color", []byte("red"), 1)
	fmt.Printf("expected error: %s\n", err)

	// revision numbers allow safe optimistic concurrency control per entries
	update, err := kv.Update(ctx, "sue.color", []byte("red"), 2)
	fmt.Println(update)
	if err != nil {
		return
	}

	entry, _ = kv.Get(ctx, "sue.color")
	fmt.Printf("%s @ %d -> %q\n", entry.Key(), entry.Revision(), string(entry.Value()))

	// kv bucket is a light abstraction over the standard NATS stream
	name := <-js.StreamNames(ctx).Name()
	fmt.Printf("KV stream name: %s\n", name)

	// first token is a special reserved prefix and the second is the bucket name,
	// and the remaining suffix is the actually key. The bucket name is inherently a namespace
	// for all keys and thus there is no concern for conflit across buckets
	cons, _ := js.CreateOrUpdateConsumer(ctx, "KV_profiles", jetstream.ConsumerConfig{
		AckPolicy: jetstream.AckNonePolicy,
	})

	msg, _ := cons.Next()
	md, _ := msg.Metadata()
	fmt.Printf("%s @ %d -> %q\n", msg.Subject(), md.Sequence.Stream, string(msg.Data()))

	put, err = kv.Put(ctx, "sue.color", []byte("yellow"))
	fmt.Println(put)
	if err != nil {
		return
	}
	msg, _ = cons.Next()
	md, _ = msg.Metadata()
	fmt.Printf("%s @ %d -> %q\n", msg.Subject(), md.Sequence.Stream, string(msg.Data()))

	err = kv.Delete(ctx, "sue.color")
	if err != nil {
		return
	}
	msg, _ = cons.Next()
	md, _ = msg.Metadata()
	fmt.Printf("%s @ %d -> %q\n", msg.Subject(), md.Sequence.Stream, msg.Data())

	// we get immediate feedback that a new revision exists and the provided header value DEL describes the operation
	fmt.Printf("headers: %v\n", msg.Headers())

	// watcher is more convenient to monitor changes to a key rather than subscribing
	w, _ := kv.Watch(ctx, "sue.*")
	defer func(w jetstream.KeyWatcher) {
		er := w.Stop()
		if er != nil {

		}
	}(w)

	put, err = kv.Put(ctx, "sue.color", []byte("purple"))
	fmt.Println(put)
	if err != nil {
		return
	}

	kve := <-w.Updates()
	fmt.Printf("%s @ %d -> %q (op: %s)\n", kve.Key(), kve.Revision(), string(kve.Value()), kve.Operation())

	<-w.Updates()

	kve = <-w.Updates()
	fmt.Printf("%s @ %d -> %q (op: %s)\n", kve.Key(), kve.Revision(), string(kve.Value()), kve.Operation())

	put, err = kv.Put(ctx, "sue.food", []byte("pizza"))
	fmt.Println(put)
	if err != nil {
		return
	}

	kve = <-w.Updates()
	fmt.Printf("%s @ %d -> %q (op: %s)\n", kve.Key(), kve.Revision(), string(kve.Value()), kve.Operation())
}
