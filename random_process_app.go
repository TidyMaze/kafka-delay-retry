package kafka_delay_retry

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func StartTestApp(inputTopic string, outputTopic string) {
	rand.Seed(time.Now().UnixNano())

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

	consumer.SubscribeTopics([]string{inputTopic}, nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to subscribe to topic: %s\n", err))
	}

	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			panic(fmt.Sprintf("Consumer error: %v (%v)\n", err, msg))
		}

		isAFailure := rand.Intn(100) < 60
		if isAFailure {
			topic := inputTopic + "-retry"
			fmt.Printf("[RandomProcessApp] Message FAILURE on %s: %s\n", msg.TopicPartition, string(msg.Value))
			copyMessageTo(topic, msg, producer, consumer)
		} else {
			topic := outputTopic
			fmt.Printf("[RandomProcessApp] Message SUCCESS on %s: %s\n", msg.TopicPartition, string(msg.Value))
			copyMessageTo(topic, msg, producer, consumer)
		}

	}
}

func copyMessageTo(topic string, msg *kafka.Message, producer *kafka.Producer, consumer *kafka.Consumer) {
	delivery_chan := make(chan kafka.Event, 10000)
	message := kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:     []byte(msg.Key),
		Value:   []byte(msg.Value),
		Headers: msg.Headers,
	}

	producer.Produce(&message, delivery_chan)

	_, error := consumer.CommitMessage(msg)
	if error != nil {
		panic(fmt.Sprintf("Commit error: %v (%v)\n", error, msg))
	}
}
