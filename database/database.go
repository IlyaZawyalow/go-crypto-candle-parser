package database

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/adshao/go-binance/v2"
	_ "github.com/lib/pq"
)

func GetLatestOpenTime(db *sql.DB, tableName, symbol string) (int64, error) {
	// SQL-запрос для получения максимальной даты OpenTime для данного Symbol
	query := fmt.Sprintf("SELECT MAX(OpenTime) FROM %s WHERE Symbol = $1", tableName)

	var openTime sql.NullTime

	// Выполнение запроса
	err := db.QueryRow(query, symbol).Scan(&openTime)
	if err != nil {
		if err == sql.ErrNoRows {
			// Если данных нет, возвращаем 0 и nil как ошибку
			return 0, nil
		}
		// В случае ошибки запроса возвращаем ошибку
		return 0, err
	}

	// Проверяем, если результат был NULL
	if !openTime.Valid {
		// Если значение NULL, возвращаем 0
		return 0, nil
	}

	// Возвращаем время в формате Unix миллисекунды
	return openTime.Time.UnixMilli(), nil
}

func SaveKlines(klines []*binance.Kline, symbol string, tableName string, db *sql.DB) {
	for _, kline := range klines {

		// Подготовленный запрос с параметрами
		klineQuery := fmt.Sprintf(
			"INSERT INTO %s (OpenTime, Symbol, Open, High, Low, Close, Volume, CloseTime, QuoteAssetVolume, TradeNum, TakerBuyBaseAssetVolume, TakerBuyQuoteAssetVolume) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
			tableName,
		)

		// Выполнение запроса
		_, err := db.Exec(klineQuery, time.UnixMilli(kline.OpenTime), symbol, kline.Open, kline.High, kline.Low, kline.Close, kline.Volume, time.UnixMilli(kline.CloseTime), kline.QuoteAssetVolume, kline.TradeNum, kline.TakerBuyBaseAssetVolume, kline.TakerBuyQuoteAssetVolume)
		if err != nil {
			fmt.Println(err)
		}
	}
	fmt.Println("Данные успошно сохранены")

}

func ConnectToDatabase() (*sql.DB, error) {
	userName := os.Getenv("UserName")
	bdName := os.Getenv("DbName")
	password := os.Getenv("Password")
	sslmode := os.Getenv("Sslmode")
	port := os.Getenv("DbPort")

	// Добавляем порт в строку подключения
	connStr := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=%s port=%s", userName, password, bdName, sslmode, port)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	return db, nil
}
