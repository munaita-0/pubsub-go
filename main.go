package main

// ref: https://github.com/GoogleCloudPlatform/golang-samples/blob/master/pubsub/topics/main.go
// ref: https://github.com/GoogleCloudPlatform/golang-samples/blob/master/pubsub/subscriptions/main.go

import (
	"fmt"
	"log"
	"os"
	"sync"

	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

func main() {
	ctx := context.Background()
	proj := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if proj == "" {
		fmt.Fprint(os.Stderr, "GOOGLE_CLOUD_PROJECT environment variable must be set.\n")
		os.Exit(1)
	}

	client, err := pubsub.NewClient(ctx, proj)
	if err != nil {
		log.Fatalf("Could not create pubsub Client: %v", err)
	}

	fmt.Println("Listing all topics from the project:")
	topics, err := list(client)
	if err != nil {
		log.Fatalf("Failed to list topics: %v", err)
	}
	for _, t := range topics {
		fmt.Println(t)
	}

	fmt.Println("Listing all subscriptions from the project:")
	subs, err := listSubscription(client, "my-topic")
	if err != nil {
		log.Fatalf("Failed to listSubscriptions: %v", err)
	}
	for _, s := range subs {
		fmt.Println(s)
	}

	fmt.Println("PUBLISH:")
	if err := publish(client, "my-topic", "hello world!"); err != nil {
		log.Fatalf("Failed to publish: %v", err)
	}

	t := client.Topic("my-topic")
	if err := pullMsgs(client, "my-sub", t); err != nil {
		log.Fatal(err)
	}
}

func list(client *pubsub.Client) ([]*pubsub.Topic, error) {
	ctx := context.Background()

	var topics []*pubsub.Topic

	it := client.Topics(ctx)
	for {
		topic, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		topics = append(topics, topic)
	}

	return topics, nil
}

func listSubscription(client *pubsub.Client, topicID string) ([]*pubsub.Subscription, error) {
	ctx := context.Background()

	var subs []*pubsub.Subscription

	it := client.Topic(topicID).Subscriptions(ctx)

	for {
		sub, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		subs = append(subs, sub)
	}
	return subs, nil
}

func publish(client *pubsub.Client, topicId string, msg string) error {
	ctx := context.Background()
	t := client.Topic(topicId)
	result := t.Publish(ctx, &pubsub.Message{
		Data: []byte(msg),
	})

	id, err := result.Get(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("Published a message; msg ID: %v\n", id)
	return nil
}

func pullMsgs(client *pubsub.Client, subName string, topic *pubsub.Topic) error {
	ctx := context.Background()

	var mu sync.Mutex
	received := 0
	sub := client.Subscription(subName)
	cctx, cancel := context.WithCancel(ctx)
	err := sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		msg.Ack()
		fmt.Printf("Got message: %q\n", string(msg.Data))
		mu.Lock()
		defer mu.Unlock()
		received++
		if received == 10 {
			cancel()
		}
	})

	if err != nil {
		return err
	}

	return nil
}
