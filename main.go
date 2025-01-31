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
	url := os.Getenv("NATS_SERVICE")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, _ := nats.Connect(url)
	defer nc.Drain()

	js, _ := jetstream.New(nc)

        // JetStream API uses context for timeouts and cancellation
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

        // a key-value bucket is created in NATS
	kv, _ := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "profiles",
	})

	kv.Put(ctx, "sue.color", []byte("blue"))
	entry, _ := kv.Get(ctx, "sue.color")
	
	// revision number of the entries will always be tracked
	fmt.Printf("%s @ %d -> %q\n", entry.Key(), entry.Revision(), string(entry.Value()))

	kv.Put(ctx, "sue.color", []byte("green"))
	entry, _ = kv.Get(ctx, "sue.color")
	fmt.Printf("%s @ %d -> %q\n", entry.Key(), entry.Revision(), string(entry.Value()))

	_, err := kv.Update(ctx, "sue.color", []byte("red"), 1)
	fmt.Printf("expected error: %s\n", err)

	// revision numbers allow safe optimistic concurrency control per entries
	kv.Update(ctx, "sue.color", []byte("red"), 2)
	entry, _ = kv.Get(ctx, "sue.color")
	fmt.Printf("%s @ %d -> %q\n", entry.Key(), entry.Revision(), string(entry.Value()))

	// kv bucket is a light abstraction over the standard NATS stream
	name := <-js.StreamNames(ctx).Name()
	fmt.Printf("KV stream name: %s\n", name)

	// 
	cons, _ := js.CreateOrUpdateConsumer(ctx, "KV_profiles", jetstream.ConsumerConfig{
		AckPolicy: jetstream.AckNonePolicy,
	})

	msg, _ := cons.Next()
	md, _ := msg.Metadata()
	fmt.Printf("%s @ %d -> %q\n", msg.Subject(), md.Sequence.Stream, string(msg.Data()))

	kv.Put(ctx, "sue.color", []byte("yellow"))
	msg, _ = cons.Next()
	md, _ = msg.Metadata()
	fmt.Printf("%s @ %d -> %q\n", msg.Subject(), md.Sequence.Stream, string(msg.Data()))

	kv.Delete(ctx, "sue.color")
	msg, _ = cons.Next()
	md, _ = msg.Metadata()
	fmt.Printf("%s @ %d -> %q\n", msg.Subject(), md.Sequence.Stream, msg.Data())

	fmt.Printf("headers: %v\n", msg.Headers())

	w, _ := kv.Watch(ctx, "sue.*")
	defer w.Stop()

	kv.Put(ctx, "sue.color", []byte("purple"))

	kve := <-w.Updates()
	fmt.Printf("%s @ %d -> %q (op: %s)\n", kve.Key(), kve.Revision(), string(kve.Value()), kve.Operation())

	<-w.Updates()

	kve = <-w.Updates()
	fmt.Printf("%s @ %d -> %q (op: %s)\n", kve.Key(), kve.Revision(), string(kve.Value()), kve.Operation())

	kv.Put(ctx, "sue.food", []byte("pizza"))

	kve = <-w.Updates()
	fmt.Printf("%s @ %d -> %q (op: %s)\n", kve.Key(), kve.Revision(), string(kve.Value()), kve.Operation())
}
