package main

import (
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type Product struct {
	gorm.Model
	Code  string
	Price uint
}

func main() {
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
}
