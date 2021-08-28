package main

import (
	"fmt"
	"runtime"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type Product struct {
	gorm.Model
	Code  string
	Price uint
}

func startConsumingMessages(c *kafka.Consumer) {
	// show start of consumer
	fmt.Println("Starting consumer")
	defer func() {
		fmt.Println("End of consumer")
	}()
	for {
		msg, err := c.ReadMessage(-1)
		if err != nil {
			panic(fmt.Sprintf("Consumer error: %v (%v)\n", err, msg))
		}

		fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))

		_, error := c.CommitMessage(msg)
		if error != nil {
			panic(fmt.Sprintf("Commit error: %v (%v)\n", error, msg))
		}
	}

}

func main() {
	// print current GOMAXPROCS from runtime
	fmt.Printf("GOMAXPROCS: %d\n", runtime.GOMAXPROCS(0))

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

	// print start of kafka test
	fmt.Println("Starting kafka test")

	topic := "test"

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092",
		"client.id":         "test",
	})

	if err != nil {
		panic(fmt.Sprintf("Failed to create producer: %s\n", err))
	}

	defer func() {
		fmt.Println("Producer cleanup")
		p.Close()
	}()

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost:29092",
		"group.id":           "kafka-delay-retry",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	})

	if err != nil {
		panic(fmt.Sprintf("Failed to create consumer: %s\n", err))
	}

	defer func() {
		fmt.Println("Consumer cleanup")
		err := c.Close()
		if err != nil {
			panic(fmt.Sprintf("Error closing consumer: %v", err))
		}
	}()

	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to subscribe to topic: %s\n", err))
	}

	go startConsumingMessages(c)

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
		// sleep 500ms to simulate a delay
		time.Sleep(500 * time.Millisecond)
	}
	// show end of producer
	fmt.Println("End of producer")

	remaining := p.Flush(1000)
	fmt.Printf("%d messages remaining in producer queue\n", remaining)

	// show why we are sleeping
	fmt.Println("Sleeping for 10 seconds")
	time.Sleep(10 * time.Second)

	// show end of program
	fmt.Println("End of kafka test")
}
