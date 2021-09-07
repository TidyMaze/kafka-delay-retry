package kafka_delay_retry

func main() {
	app := KafkaDelayRetryApp{
		config: KafkaDelayRetryConfig{
			inputTopic:       "^.*-retry",
			bootstrapServers: "localhost:9092",
		},
	}
	app.start()
}
