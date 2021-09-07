package kafka_delay_retry

import (
	"fmt"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

// testing main app
func TestApp(t *testing.T) {
	inputTopic := "test-app-input-topic"
	retryTopic := "^.*-retry"
	outputTopic := "test-app-output-topic"

	config := KafkaDelayRetryConfig{
		inputTopic:       retryTopic,
		bootstrapServers: "localhost:29092",
	}

	app := NewKafkaDelayRetryApp(config)

	app.messageRepository.Truncate()

	app.start()

	sizeProduced := 100

	produceTestMessages(inputTopic, sizeProduced)

	go StartTestApp(inputTopic, outputTopic, config.bootstrapServers)

	messages := expectMessages(t, outputTopic, 5*time.Minute, sizeProduced)

	for _, msg := range messages {
		fmt.Printf("%s\n", msg.Value)
	}

	app.stop()
}

func expectMessages(t assert.TestingT, topic string, maxWaitForMessage time.Duration, expectedSize int) []kafka.Message {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092",
		"group.id":          "test-consumer",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(fmt.Sprintf("Failed to create consumer: %s\n", err))
	}

	defer func() {
		fmt.Println("readMessages cleanup")
		c.Close()
	}()

	fmt.Printf("Created consumer %v\n", c)

	err = c.SubscribeTopics([]string{topic}, nil)

	if err != nil {
		panic(fmt.Sprintf("Failed to subscribe to topic: %s\n", err))
	}

	messages := make([]kafka.Message, 0)

	for {
		msg, err := c.ReadMessage(maxWaitForMessage)

		if err != nil && err.(kafka.Error).Code() == kafka.ErrTimedOut {
			return messages
		} else if err == nil {
			fmt.Printf("[readMessages] Received message in topic %s: %s\n", topic, string(msg.Value))
			messages = append(messages, *msg)

			if len(messages) == expectedSize {
				return messages
			} else if len(messages) > expectedSize {
				assert.Fail(t, fmt.Sprintf("Expected %d messages, but got %d\n", expectedSize, len(messages)))
			}
		} else {
			panic(fmt.Sprintf("Failed to read message: %s\n", err))
		}
	}
}

func produceTestMessages(topic string, size int) {

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

	for i := 0; i < size; i++ {
		fmt.Printf("Producing message to topic %s: %d\n", topic, i)
		err := p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(fmt.Sprintf("%d-test", i))}, delivery_chan)

		if err != nil {
			panic(fmt.Sprintf("Failed to produce message: %s\n", err))
		}
	}
	fmt.Println("End of producer")

	remaining := p.Flush(1000)
	fmt.Printf("%d messages remaining in producer queue\n", remaining)
}
