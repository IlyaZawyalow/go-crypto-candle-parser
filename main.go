package main

import (
	"KlineParse/database"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/adshao/go-binance/v2"
	"github.com/joho/godotenv"
)

var KlineRequestWeight int = 22
var LimitWeight int = 6000

type Parser struct {
	c           *binance.Client
	db          *sql.DB
	Symbols     []string
	Limit       int
	Interval    string
	StartTime   int64
	EndTime     int64
	WorkerLimit int
}

// Инициализация парсера
func InitParser(db *sql.DB, Limit int) *Parser {
	// Получаем символы из переменной окружения
	symbolsStr := os.Getenv("BINANCE_SYMBOLS")
	if symbolsStr == "" {
		log.Fatal("BINANCE_SYMBOLS is not set")
	}
	symbols := strings.Split(symbolsStr, ",")

	// Получаем начальные и конечные даты
	Start, _ := time.Parse(time.RFC3339, os.Getenv("DataStart"))
	End, _ := time.Parse(time.RFC3339, os.Getenv("DataEnd"))
	StartTime := Start.UnixMilli()
	EndTime := End.UnixMilli()

	// Инициализируем клиент Binance (убедитесь, что у вас есть API ключи в переменных окружения)
	client := binance.NewClient("", "")
	WorkerLimit := WorkerLimit(len(symbols))
	return &Parser{
		c:           client,
		db:          db,
		Symbols:     symbols,
		Limit:       Limit,
		Interval:    "1m",
		StartTime:   StartTime,
		EndTime:     EndTime,
		WorkerLimit: WorkerLimit,
	}
}

// Рассчитываем количество воркеров
func WorkerLimit(n int) int {
	return int(float32(LimitWeight) / (float32(n) * float32(KlineRequestWeight) * 1.3))
}

func main() {
	// Загружаем переменные окружения
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Подключаемся к базе данных
	db, err := database.ConnectToDatabase()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("Успешное подключение к базе данных!")

	// Инициализируем парсер
	p := InitParser(db, 1000)
	p.StartParser()
}

// Старт парсера
func (p *Parser) StartParser() {
	var wg sync.WaitGroup

	// Интервал для тикера определяется на основе лимита воркеров
	interval := time.Millisecond * time.Duration(60000/p.WorkerLimit)

	for _, symbol := range p.Symbols {
		wg.Add(1)
		go func(symbol string) {
			defer wg.Done()
			// Создаем тикер для каждой горутины
			ticker := time.NewTicker(interval)
			defer ticker.Stop() // Убедитесь, что тикер останавливается при завершении

			p.ParseSymbol(symbol, ticker)
		}(symbol)
	}
	wg.Wait()
	fmt.Println("Все воркеры завершены")
}

// Парсинг символа
func (p *Parser) ParseSymbol(symbol string, ticker *time.Ticker) {
	// Получаем последний OpenTime для указанного символа
	startTime, err := database.GetLatestOpenTime(p.db, os.Getenv("DbTableName"), symbol)
	if err != nil {
		fmt.Println("При получении даты из БД произошла ошибка:", err)
		return
	}

	startTime = 0

	// Если нет данных в БД, используем начальное время из конфигурации
	if startTime == 0 {
		startTime = p.StartTime
	}

	fmt.Printf("startTime: %v, Валюта: %v \n", startTime, symbol)

	// Основной цикл для обработки свечей
	for {
		// Ожидаем сигнала от тикера для выполнения запроса
		<-ticker.C

		// Запрос данных о свечах с использованием startTime в формате int64 (Unix миллисекунды)
		klines, err := p.c.NewKlinesService().
			Symbol(symbol).
			Interval(p.Interval).
			StartTime(startTime).
			EndTime(p.EndTime).
			Limit(p.Limit).
			Do(context.Background())
		if err != nil {
			fmt.Println("Ошибка при запросе свечей:", err)
			return
		}

		if len(klines) == 0 {
			fmt.Println("Нет данных для обработки")
			break
		}

		// Сохранение свечей в базу данных
		database.SaveKlines(klines, symbol, os.Getenv("DbTableName"), p.db)

		// Обновляем startTime на основе последнего времени в запросе
		lastKline := klines[len(klines)-1]
		startTime = lastKline.CloseTime

		// Проверяем, если новое startTime превышает конечное время
		if startTime >= p.EndTime {
			break
		}
	}
}
