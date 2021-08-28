package kafka_delay_retry

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type Product struct {
	gorm.Model
	Code  string
	Price uint
}

type KafkaDelayRetryConfig struct {
	inputTopic       string
	outputTopic      string
	bootstrapServers string
}

type KafkaDelayRetryApp struct {
	config   KafkaDelayRetryConfig
	consumer *kafka.Consumer
	// producer   *kafka.Producer
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

		_, error := a.consumer.CommitMessage(msg)
		if error != nil {
			panic(fmt.Sprintf("Commit error: %v (%v)\n", error, msg))
		}
	}

}

func main() {
	inputTopic := "test"
	outputTopic := "test-output"
	app := KafkaDelayRetryApp{
		config: KafkaDelayRetryConfig{
			inputTopic:       inputTopic,
			outputTopic:      outputTopic,
			bootstrapServers: "localhost:9092",
		},
	}
	app.start()
}

func checkDb() {
	db, err := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
	if err != nil {
		// log err with message
		panic(fmt.Sprintf("Error opening database: %v", err))
	}

	err = db.AutoMigrate(&Product{})
	if err != nil {
		panic(fmt.Sprintf("Error auto-migrating: %v", err))
	}

	// Cleanup before running app
	err = db.Exec("DELETE FROM products").Error
	if err != nil {
		panic(fmt.Sprintf("Error cleaning up: %v", err))
	}

	err = db.Create(&Product{Code: "D42", Price: 100}).Error
	if err != nil {
		panic(fmt.Sprintf("Error creating product: %v", err))
	}

	var product Product
	err = db.First(&product, 1).Error
	if err != nil {
		panic(fmt.Sprintf("Error finding product: %v", err))
	}

	err = db.Model(&product).Update("Price", 42).Error
	if err != nil {
		panic(fmt.Sprintf("Error updating product: %v", err))
	}

	err = db.Delete(&product, 1).Error
	if err != nil {
		panic(fmt.Sprintf("Error deleting product: %v", err))
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

	// checkDb()

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
	a.subscribeTopics()

	go a.startConsumingMessages()
}

func (a *KafkaDelayRetryApp) stop() {
	fmt.Println("Consumer cleanup")
	err := a.consumer.Close()
	if err != nil {
		panic(fmt.Sprintf("Error closing consumer: %v", err))
	}
}
