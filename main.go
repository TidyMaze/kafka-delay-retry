package kafka_delay_retry

import "context"

func main() {
	app := KafkaDelayRetryApp{
		config: KafkaDelayRetryConfig{
			inputTopic:       "^.*-retry",
			bootstrapServers: "localhost:9092",
		},
	}

	ctx := context.Background()

	appCtx, cancel := context.WithCancel(ctx)

	defer cancel()

	app.start(appCtx)
}
