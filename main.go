package main

import (
	"context"

	"github.com/TidyMaze/kafka-delay-retry/internal"
)

func main() {
	ctx := context.Background()

	appCtx, cancel := context.WithCancel(ctx)

	internal.NewKafkaDelayRetryApp(appCtx, internal.KafkaDelayRetryConfig{
		InputTopic:       "^.*-retry",
		BootstrapServers: "localhost:9092",
	})

	defer cancel()
}
