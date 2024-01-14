package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/remiges-tech/logharbour/logharbour"
)

func main() {
	nGoroutines := 100
	nMessages := 100000
	messagesPerGoroutine := nMessages / nGoroutines

	// Initialize Kafka connection pool and LogHarbour logger
	pool, err := logharbour.NewKafkaConnectionPool(100, logharbour.KafkaConfig{
		Brokers: []string{"localhost:9092"},
	})
	if err != nil {
		panic(fmt.Sprintf("unable to create Kafka connection pool: %v", err))
	}

	topic := "log_topic"
	kafkaWriter := logharbour.NewKafkaWriter(pool, topic)

	// Assuming a function NewLoggerContext exists in your package
	lctx := logharbour.NewLoggerContext(logharbour.Info)

	// Create a fallback writer that uses stdout as the fallback.
	fallbackWriter := logharbour.NewFallbackWriter(kafkaWriter, os.Stdout)

	var wg sync.WaitGroup
	wg.Add(nGoroutines)

	// Start measuring time
	startTime := time.Now()

	for i := 0; i < nGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			appName := fmt.Sprintf("MyApp-%d", id)

			// Assuming a function NewLoggerWithFallback exists in your package
			logger := logharbour.NewLoggerWithFallback(lctx, appName, fallbackWriter)

			for j := 0; j < messagesPerGoroutine; j++ {
				message := fmt.Sprintf("Goroutine %d: message %d", id, j)
				// wait for 1 ms
				// this is the only way i can get around the fact that disk i/o is the bottleneck
				time.Sleep(1 * time.Millisecond)
				logMessage(logger, message)
			}
		}(i)
	}

	wg.Wait()

	// Stop measuring time
	duration := time.Since(startTime)

	// Close Kafka writer when done
	if err := kafkaWriter.Close(); err != nil {
		panic(fmt.Sprintf("failed to close Kafka writer: %v", err))
	}

	// Compute and display metrics
	fmt.Printf("Total execution time: %v\n", duration)
	fmt.Printf("Messages per second: %f\n", float64(nMessages)/duration.Seconds())
}

func logMessage(logger *logharbour.Logger, message string) {
	// Replace with actual logging logic using LogHarbour
	// Assuming a method LogActivity exists in your Logger
	logger.LogActivity("Log Message", map[string]interface{}{
		"message": message,
	})
}
