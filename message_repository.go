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
	err := mr.db.AutoMigrate(&StoredMessage{})
	if err != nil {
		panic(fmt.Sprintf("Error auto-migrating: %v", err))
	}
}

// truncate db table
func (mr *MessageRepository) Truncate() {
	// Cleanup before running app
	err := mr.db.Exec("DELETE FROM stored_messages").Error
	if err != nil {
		panic(fmt.Sprintf("Error cleaning up: %v", err))
	}
}

func (mr *MessageRepository) Create(message StoredMessage) {
	err := mr.db.Create(&message).Error
	if err != nil {
		panic(fmt.Sprintf("Error saving message: %v", err))
	}
}

func (mr *MessageRepository) FindById(id int) StoredMessage {
	var msg StoredMessage
	err := mr.db.First(&msg, id).Error
	if err != nil {
		panic(fmt.Sprintf("Error finding message: %v", err))
	}
	return msg
}
