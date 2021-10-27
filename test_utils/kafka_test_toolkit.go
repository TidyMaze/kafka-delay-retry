package test_utils

import (
	"fmt"
	"sort"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

func ProduceTestMessages(topic string, size int) {

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

func ExpectMessages(t assert.TestingT, topic string, maxWaitForMessage time.Duration, expectedSize int) {
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

	values := []string{}

	for {
		msg, err := c.ReadMessage(maxWaitForMessage)

		if err != nil && err.(kafka.Error).Code() == kafka.ErrTimedOut {
			assert.Fail(t, "should have returned before with all values")
			return
		} else if err == nil {

			values = append(values, string(msg.Value))

			fmt.Printf("[readMessages] Received message in topic %s: %s (%d yet)\n", topic, string(msg.Value), len(values))

			sort.Strings(values)

			if len(unique(values)) != len(values) {
				assert.FailNow(t, "Duplicate messages found%v\n", values)
			}

			if len(values) == expectedSize {
				return
			} else if len(values) > expectedSize {
				assert.Fail(t, fmt.Sprintf("Expected %d messages, but got %d\n", expectedSize, len(values)))
			}
		} else {
			panic(fmt.Sprintf("Failed to read message: %s\n", err))
		}
	}
}

func unique(slice []string) []string {
	keys := make(map[string]bool)
	list := []string{}
	for _, entry := range slice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}
