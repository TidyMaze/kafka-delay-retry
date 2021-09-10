package kafka_delay_retry

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaDelayRetryApp struct {
	config            KafkaDelayRetryConfig
	consumer          *kafka.Consumer
	producer          *kafka.Producer
	messageRepository *MessageRepository
	cancelFn          context.CancelFunc
}

const RETRY_HEADER_KEY = "retry-duration"
const APP_NAME = "kafka-delay-retry"

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
			msg, err := a.consumer.ReadMessage(-1)
			if err != nil {
				panic(fmt.Sprintf("[Retry] Consumer error: %v (%v)\n", err, msg))
			}

			fmt.Printf("[Retry] Message on %s: %s with headers %v\n", msg.TopicPartition, string(msg.Value), msg.Headers)

			waitDuration := time.Duration(100) * time.Millisecond
			if msg.Headers != nil {
				for _, header := range msg.Headers {
					if string(header.Key) == RETRY_HEADER_KEY {
						intDuration, _ := strconv.Atoi(string(header.Value))
						// random duration offset between 0 and 1000ms
						offset := time.Duration(intDuration+int(time.Duration(rand.Intn(1000)))) * time.Millisecond
						waitDuration = time.Duration(intDuration)*2*time.Millisecond + offset
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

func (a *KafkaDelayRetryApp) subscribeTopics() {
	err := a.consumer.SubscribeTopics([]string{a.config.inputTopic}, nil)
	if err != nil {
		panic(fmt.Sprintf("[Retry] Failed to subscribe to topic: %s\n", err))
	}
}

func (a *KafkaDelayRetryApp) start() {
	fmt.Println("[Retry] Starting app")

	newConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  a.config.bootstrapServers,
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
		"bootstrap.servers": a.config.bootstrapServers,
		"client.id":         APP_NAME,
	})

	if err != nil {
		panic(fmt.Sprintf("[Retry] Failed to create producer: %s\n", err))
	}

	a.producer = newProducer

	a.subscribeTopics()

	ctx := context.Background()

	ctx2, cancelFn := context.WithCancel(ctx)
	a.cancelFn = cancelFn

	go a.startExpiredMessagesPolling(ctx2)
	go a.startConsumingMessages(ctx2)
}

func (a *KafkaDelayRetryApp) stop() {
	a.cancelFn()

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
}

func NewKafkaDelayRetryApp(config KafkaDelayRetryConfig) *KafkaDelayRetryApp {
	return &KafkaDelayRetryApp{
		config:            config,
		messageRepository: SqliteMessageRepository(),
	}
}

func (a *KafkaDelayRetryApp) startExpiredMessagesPolling(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			expiredMessages := a.messageRepository.FindAllExpired(10)

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
