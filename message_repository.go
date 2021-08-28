package kafka_delay_retry

import (
	"fmt"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type MessageRepository struct {
	db *gorm.DB
}

func NewMessageRepository() *MessageRepository {
	db, err := gorm.Open(sqlite.Open("database.db"), &gorm.Config{})
	if err != nil {
		// log err with message
		panic(fmt.Sprintf("Error opening database: %v", err))
	}

	mr := &MessageRepository{
		db: db,
	}

	mr.migrate()

	return mr
}

func (mr *MessageRepository) migrate() {
	err := mr.db.AutoMigrate(&Product{})
	if err != nil {
		panic(fmt.Sprintf("Error auto-migrating: %v", err))
	}
}

// truncate db table
func (mr *MessageRepository) Truncate() {
	// Cleanup before running app
	err := mr.db.Exec("DELETE FROM products").Error
	if err != nil {
		panic(fmt.Sprintf("Error cleaning up: %v", err))
	}
}

func (mr *MessageRepository) Create(product *Product) {
	err := mr.db.Create(product).Error
	if err != nil {
		panic(fmt.Sprintf("Error creating product: %v", err))
	}
}

func (mr *MessageRepository) FindById(id int) Product {
	var product Product
	err := mr.db.First(&product, id).Error
	if err != nil {
		panic(fmt.Sprintf("Error finding product: %v", err))
	}
	return product
}
