package internal

import (
	"fmt"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type MessageRepository struct {
	Db *gorm.DB
}

func SqliteMessageRepository() *MessageRepository {
	return SqliteMessageRepositoryFromFile("database.db")
}

func SqliteMessageRepositoryFromFile(file string) *MessageRepository {
	return NewMessageRepository(sqlite.Open(file))
}

func NewMessageRepository(dialector gorm.Dialector) *MessageRepository {
	db, err := gorm.Open(dialector, &gorm.Config{})
	if err != nil {
		// log err with message
		panic(fmt.Sprintf("Error opening database: %v", err))
	}

	mr := &MessageRepository{
		Db: db,
	}

	mr.migrate()

	return mr
}

func (mr *MessageRepository) migrate() {
	err := mr.Db.AutoMigrate(&StoredMessage{})
	if err != nil {
		panic(fmt.Sprintf("Error auto-migrating: %v", err))
	}
}

// truncate db table
func (mr *MessageRepository) Truncate() {
	// Cleanup before running app
	err := mr.Db.Exec("DELETE FROM stored_messages").Error
	if err != nil {
		panic(fmt.Sprintf("Error cleaning up: %v", err))
	}
}

func (mr *MessageRepository) Create(message *StoredMessage) {
	err := mr.Db.Create(message).Error
	if err != nil {
		panic(fmt.Sprintf("Error saving message: %v", err))
	}
}

func (mr *MessageRepository) FindById(id int) StoredMessage {
	var msg StoredMessage
	err := mr.Db.First(&msg, id).Error
	if err != nil {
		panic(fmt.Sprintf("Error finding message: %v", err))
	}
	return msg
}

// find all stored messages who WaitUntil is less than or equal to now
func (mr *MessageRepository) FindAllExpired(limit int) []StoredMessage {
	var msgs []StoredMessage
	err := mr.Db.Where("wait_until <= ?", time.Now()).Order("wait_until ASC").Limit(limit).Find(&msgs).Error
	if err != nil {
		panic(fmt.Sprintf("Error finding messages: %v", err))
	}
	return msgs
}

// delete a stored message
func (mr *MessageRepository) Delete(msg StoredMessage) {
	err := mr.Db.Delete(&msg).Error
	if err != nil {
		panic(fmt.Sprintf("Error deleting message: %v", err))
	}
}
