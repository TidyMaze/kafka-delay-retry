package kafka_delay_retry

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaDelayRetryConfig struct {
	inputTopic       string
	outputTopic      string
	bootstrapServers string
}

type KafkaDelayRetryApp struct {
	config            KafkaDelayRetryConfig
	consumer          *kafka.Consumer
	producer          *kafka.Producer
	messageRepository *MessageRepository
}

func (a *KafkaDelayRetryApp) startConsumingMessages() {
	// show start of consumer
	fmt.Println("Starting consumer")
	defer func() {
		fmt.Println("End of consumer")
	}()
	for {
		msg, err := a.consumer.ReadMessage(-1)
		if err != nil {
			panic(fmt.Sprintf("Consumer error: %v (%v)\n", err, msg))
		}

		fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))

		sm := StoredMessage{
			Message: *msg,
		}

		a.messageRepository.Create(sm)

		delivery_chan := make(chan kafka.Event, 10000)

		a.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &a.config.outputTopic,
				Partition: kafka.PartitionAny,
			},
			Key:   msg.Key,
			Value: msg.Value,
		}, delivery_chan)

		_, error := a.consumer.CommitMessage(msg)
		if error != nil {
			panic(fmt.Sprintf("Commit error: %v (%v)\n", error, msg))
		}
	}

}

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

func (a *KafkaDelayRetryApp) subscribeTopics() {
	err := a.consumer.SubscribeTopics([]string{a.config.inputTopic}, nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to subscribe to topic: %s\n", err))
	}
}

func (a *KafkaDelayRetryApp) start() {
	fmt.Println("Starting app")

	a.messageRepository = NewMessageRepository()

	newConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  a.config.bootstrapServers,
		"group.id":           "kafka-delay-retry",
		"client.id":          "kafka-delay-retry",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	})

	if err != nil {
		panic(fmt.Sprintf("Failed to create consumer: %s\n", err))
	}

	a.consumer = newConsumer

	newProducer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": a.config.bootstrapServers,
		"client.id":         "kafka-delay-retry",
	})

	if err != nil {
		panic(fmt.Sprintf("Failed to create producer: %s\n", err))
	}

	a.producer = newProducer

	a.subscribeTopics()

	go a.startConsumingMessages()
}

func (a *KafkaDelayRetryApp) stop() {
	fmt.Println("Consumer cleanup")
	err := a.consumer.Close()
	if err != nil {
		panic(fmt.Sprintf("Error closing consumer: %v", err))
	}

	fmt.Println("Producer cleanup")
	a.producer.Close()
	if err != nil {
		panic(fmt.Sprintf("Error closing producer: %v", err))
	}
}
