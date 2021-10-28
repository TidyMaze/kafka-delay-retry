package internal

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaDelayRetryApp struct {
	config            KafkaDelayRetryConfig
	consumer          *kafka.Consumer
	producer          *kafka.Producer
	messageRepository *MessageRepository
}

const RETRY_HEADER_KEY = "retry-duration"
const APP_NAME = "kafka-delay-retry"

func IncreaseRetryDuration(waitDuration time.Duration) time.Duration {
	maxRetryDuration := time.Duration(24) * time.Hour

	res := waitDuration * 2
	if res > time.Duration(maxRetryDuration) {
		return time.Duration(maxRetryDuration)
	}
	return res
}

func (a *KafkaDelayRetryApp) startConsumingMessages(ctx context.Context) {
	fmt.Println("[Retry] Starting consumer")
	defer func() {
		fmt.Println("[Retry] End of consumer")
	}()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := a.consumer.ReadMessage(1000 * time.Millisecond)

			if err != nil && err.(kafka.Error).Code() == kafka.ErrTimedOut {
				fmt.Println("[Retry] No new message")
				continue
			} else if err != nil && err.(kafka.Error).Code() != kafka.ErrTimedOut {
				panic(fmt.Sprintf("[Retry] Consumer error: %v (%v)\n", err, msg))
			} else {
				fmt.Printf("[Retry] Message on %s: %s with headers %v\n", msg.TopicPartition, string(msg.Value), msg.Headers)

				waitDuration := time.Duration(100) * time.Millisecond
				if msg.Headers != nil {
					for _, header := range msg.Headers {
						if string(header.Key) == RETRY_HEADER_KEY {
							intDuration, _ := strconv.Atoi(string(header.Value))
							waitDuration = IncreaseRetryDuration(time.Duration(intDuration) * time.Millisecond)
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
					panic(fmt.Sprintf("[Retry] Commit error: %v (%v)\n", error, msg))
				}
			}
		}
	}

}

func (a *KafkaDelayRetryApp) subscribeTopics() {
	err := a.consumer.SubscribeTopics([]string{a.config.InputTopic}, nil)
	if err != nil {
		panic(fmt.Sprintf("[Retry] Failed to subscribe to topic: %s\n", err))
	}
}

func (a *KafkaDelayRetryApp) start(ctx context.Context) {
	fmt.Println("[Retry] Starting app")

	newConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  a.config.BootstrapServers,
		"group.id":           APP_NAME,
		"client.id":          APP_NAME,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	})

	if err != nil {
		panic(fmt.Sprintf("[Retry] Failed to create consumer: %s\n", err))
	}

	a.consumer = newConsumer

	newProducer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": a.config.BootstrapServers,
		"client.id":         APP_NAME,
	})

	if err != nil {
		panic(fmt.Sprintf("[Retry] Failed to create producer: %s\n", err))
	}

	a.producer = newProducer

	a.subscribeTopics()

	go a.startExpiredMessagesPolling(ctx)
	go a.startConsumingMessages(ctx)
}

func (a *KafkaDelayRetryApp) stop() {
	fmt.Println("[Retry] Consumer cleanup")
	err := a.consumer.Close()
	if err != nil {
		panic(fmt.Sprintf("[Retry] Error closing consumer: %v", err))
	}

	fmt.Println("[Retry] Producer cleanup")
	a.producer.Close()
	if err != nil {
		panic(fmt.Sprintf("[Retry] Error closing producer: %v", err))
	}

	innerDb, err := a.messageRepository.Db.DB()
	if err != nil {
		panic(fmt.Sprintf("[Retry] Error closing inner db: %v", err))
	}
	err = innerDb.Close()

	if err != nil {
		panic(fmt.Sprintf("[Retry] Error closing inner db: %v", err))
	}
	fmt.Println("[Retry] Closed DB")

}

func NewKafkaDelayRetryApp(ctx context.Context, config KafkaDelayRetryConfig) {
	app := &KafkaDelayRetryApp{
		config:            config,
		messageRepository: SqliteMessageRepository(),
	}

	app.start(ctx)
}

func (a *KafkaDelayRetryApp) startExpiredMessagesPolling(ctx context.Context) {
	defer func() {
		fmt.Println("[Retry] Expiration polling stopped")
	}()
	for {
		select {
		case <-ctx.Done():
			a.stop()
			return
		default:
			expiredMessages := a.messageRepository.FindAllExpired(1000)

			if len(expiredMessages) > 0 {
				fmt.Printf("[Retry] =========== New batch of %v messages\n", len(expiredMessages))
			} else {
				fmt.Printf(".")
			}

			for _, message := range expiredMessages {
				fmt.Printf("[Retry] Retrying expired message %v with duration %v\n", message.Value, message.WaitDuration)

				delivery_chan := make(chan kafka.Event, 10000)

				retryDurationHeaderValue := strconv.Itoa(int(message.WaitDuration.Milliseconds()))

				outputTopic := message.Topic[:len(message.Topic)-len("-retry")]

				a.producer.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{
						Topic:     &outputTopic,
						Partition: kafka.PartitionAny,
					},
					Key:   []byte(message.Key),
					Value: []byte(message.Value),
					Headers: []kafka.Header{
						{
							Key:   RETRY_HEADER_KEY,
							Value: []byte(retryDurationHeaderValue),
						},
					},
				}, delivery_chan)

				a.messageRepository.Delete(message)
			}
		}

		time.Sleep(time.Millisecond * 1000)
	}
}
