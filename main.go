package kafka_delay_retry

func main() {
	app := KafkaDelayRetryApp{
		config: KafkaDelayRetryConfig{
			inputTopic:       "test",
			outputTopic:      "test-output",
			bootstrapServers: "localhost:9092",
		},
	}
	app.start()
}
