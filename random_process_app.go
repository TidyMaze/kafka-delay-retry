package kafka_delay_retry

import (
	"fmt"
	"math/rand"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func StartTestApp() {
	fmt.Println("Starting test app")
	defer func() {
		fmt.Println("End of test app consumer")
	}()

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost:29092",
		"group.id":           "test-app",
		"client.id":          "test-app",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	})

	if err != nil {
		panic(fmt.Sprintf("Failed to create consumer: %s\n", err))
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092",
		"client.id":         "test-app",
	})

	if err != nil {
		panic(fmt.Sprintf("Failed to create producer: %s\n", err))
	}

	consumer.SubscribeTopics([]string{"test-app-input-topic"}, nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to subscribe to topic: %s\n", err))
	}

	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			panic(fmt.Sprintf("Consumer error: %v (%v)\n", err, msg))
		}

		fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))

		delivery_chan := make(chan kafka.Event, 10000)

		// with 50% chance, send the message to the topic or commit it
		if rand.Intn(2) == 0 {
			topic := "test-app-output-topic"
			message := kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topic,
					Partition: kafka.PartitionAny,
				},
				Key:   []byte(msg.Key),
				Value: []byte(msg.Value),
				Headers: []kafka.Header{
					{Key: "kafka-retry-wait",
						Value: []byte(strconv.Itoa(1))},
				},
			}

			producer.Produce(&message, delivery_chan)

			_, error := consumer.CommitMessage(msg)
			if error != nil {
				panic(fmt.Sprintf("Commit error: %v (%v)\n", error, msg))
			}
		} else {
			topic := "test-app-input-topic-retry"
			message := kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topic,
					Partition: kafka.PartitionAny,
				},
				Key:   []byte(msg.Key),
				Value: []byte(msg.Value),
				Headers: []kafka.Header{
					{Key: "kafka-retry-wait",
						Value: []byte(strconv.Itoa(1))},
				},
			}

			producer.Produce(&message, delivery_chan)

			_, error := consumer.CommitMessage(msg)
			if error != nil {
				panic(fmt.Sprintf("Commit error: %v (%v)\n", error, msg))
			}
		}

	}
}
