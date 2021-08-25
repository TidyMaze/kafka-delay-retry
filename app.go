package main

import (
	"fmt"
	"os"
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

func main() {
	// print current GOMAXPROCS from runtime
	fmt.Printf("NumCPU: %d\n", runtime.NumCPU())

	db, err := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}

	db.AutoMigrate(&Product{})

	// Cleanup before running app
	db.Exec("DELETE FROM products")

	db.Create(&Product{Code: "D42", Price: 100})

	var product Product
	db.First(&product, 1)                 // find product with integer primary key
	db.First(&product, "code = ?", "D42") // find product with code D42

	db.Model(&product).Update("Price", 42)
	// db.Model(&product).Updates(map[string]interface{}{"Price": 200, "Code": "F42"})

	db.Delete(&product, 1)

	topic := "test"

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092",
		"client.id":         "test"})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	defer p.Close()

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092",
		"group.id":          "kafka-delay-retry",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	// defer c.Close()

	c.SubscribeTopics([]string{topic}, nil)

	go func() {
		for {
			msg, err := c.ReadMessage(-1)
			if err == nil {
				fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			} else {
				// The client will automatically try to recover from all errors.
				fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			}
			c.CommitMessage(msg)
		}
	}()

	// produce all numbers from 10 to 20 to kafka topic, prefixed by '-test'
	for i := 10; i <= 1000; i++ {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(fmt.Sprintf("-test%d", i))}, nil)
	}
	p.Flush(10000)

	time.Sleep(30 * time.Second)
}
