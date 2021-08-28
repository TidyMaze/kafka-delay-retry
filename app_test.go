package kafka_delay_retry

import (
	"fmt"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// testing main app
func TestApp(t *testing.T) {
	inputTopic := "test"

	app := KafkaDelayRetryApp{
		config: KafkaDelayRetryConfig{
			inputTopic:       inputTopic,
			bootstrapServers: "localhost:29092",
		},
	}

	app.start()

	produceTestMessages(inputTopic)

	app.stop()
}

func produceTestMessages(topic string) {

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092",
		"client.id":         "test-producer",
	})

	if err != nil {
		panic(fmt.Sprintf("Failed to create producer: %s\n", err))
	}

	defer func() {
		fmt.Println("Producer cleanup")
		p.Close()
	}()

	delivery_chan := make(chan kafka.Event, 10000)
	// produce all numbers from 10 to 20 to kafka topic, prefixed by '-test'
	for i := 0; i < 10; i++ {
		fmt.Printf("Producing message to topic %s: %d\n", topic, i)
		err := p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(fmt.Sprintf("%d-test", i))}, delivery_chan)

		if err != nil {
			panic(fmt.Sprintf("Failed to produce message: %s\n", err))
		}
	}
	// show end of producer
	fmt.Println("End of producer")

	remaining := p.Flush(1000)
	fmt.Printf("%d messages remaining in producer queue\n", remaining)
}
