package kafka_delay_retry

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaDelayRetryApp struct {
	config            KafkaDelayRetryConfig
	consumer          *kafka.Consumer
	producer          *kafka.Producer
	messageRepository *MessageRepository
}

func (a *KafkaDelayRetryApp) startConsumingMessages() {
	fmt.Println("Starting consumer")
	defer func() {
		fmt.Println("End of consumer")
	}()
	for {
		msg, err := a.consumer.ReadMessage(-1)
		if err != nil {
			panic(fmt.Sprintf("Consumer error: %v (%v)\n", err, msg))
		}

		fmt.Printf("[Retry] Message on %s: %s with headers %v\n", msg.TopicPartition, string(msg.Value), msg.Headers)

		waitDuration := time.Duration(1) * time.Second
		if msg.Headers != nil {
			for _, header := range msg.Headers {
				if string(header.Key) == "retry-duration" {
					intDuration := int64(binary.BigEndian.Uint64(header.Value))
					waitDuration = time.Duration(intDuration) * 2 * time.Second
					break
				}
			}
		}

		sm := StoredMessage{
			Key:          string(msg.Key),
			Value:        string(msg.Value),
			Topic:        *msg.TopicPartition.Topic,
			Partition:    msg.TopicPartition.Partition,
			Offset:       msg.TopicPartition.Offset,
			WaitDuration: waitDuration,
			WaitUntil:    time.Now().Add(waitDuration),
		}

		a.messageRepository.Create(&sm)
		fmt.Printf("[Retry] Stored message %v with duration %v\n", sm.Value, sm.WaitDuration)

		_, error := a.consumer.CommitMessage(msg)
		if error != nil {
			panic(fmt.Sprintf("Commit error: %v (%v)\n", error, msg))
		}
	}

}

func (a *KafkaDelayRetryApp) subscribeTopics() {
	err := a.consumer.SubscribeTopics([]string{a.config.inputTopic}, nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to subscribe to topic: %s\n", err))
	}
}

func (a *KafkaDelayRetryApp) start() {
	fmt.Println("Starting app")

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

	go a.startExpiredMessagesPolling()
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

func NewKafkaDelayRetryApp(config KafkaDelayRetryConfig) *KafkaDelayRetryApp {
	return &KafkaDelayRetryApp{
		config:            config,
		messageRepository: SqliteMessageRepository(),
	}
}

func (a *KafkaDelayRetryApp) startExpiredMessagesPolling() {
	for {
		expiredMessages := a.messageRepository.FindAllExpired()

		for _, message := range expiredMessages {
			fmt.Printf("Retrying expired message %v with duration %v\n", message.Value, message.WaitDuration)

			delivery_chan := make(chan kafka.Event, 10000)

			retryDurationHeaderValue := make([]byte, 8)

			durationInt := uint64(message.WaitDuration.Seconds())

			fmt.Printf("serializing duration %v\n", durationInt)

			binary.BigEndian.PutUint64(retryDurationHeaderValue, durationInt)

			a.producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &a.config.outputTopic,
					Partition: kafka.PartitionAny,
				},
				Key:   []byte(message.Key),
				Value: []byte(message.Value),
				Headers: []kafka.Header{
					{
						Key:   "retry-duration",
						Value: retryDurationHeaderValue,
					},
				},
			}, delivery_chan)

			a.messageRepository.Delete(message)
			fmt.Printf("[Retry] Deleted message %v\n", message.Value)
		}
		time.Sleep(time.Second * 1)
	}
}