package logharbour

import (
	"fmt"
	"os"
	"sync"
	"testing"
)

func BenchmarkLogWriting(b *testing.B) {
	// Create a Kafka connection pool
	pool, err := NewKafkaConnectionPool(10, KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "log_topic",
	})
	if err != nil {
		b.Fatalf("unable to create Kafka connection pool: %v", err)
	}

	// Create a Kafka writer
	kw := NewKafkaWriter(pool, "log_topic")

	// Create a fallback writer that uses stdout as the fallback.
	fallbackWriter := NewFallbackWriter(kw, os.Stdout)

	// Create a logger context with the default priority.
	lctx := NewLoggerContext(Info)

	// Initialize the logger with the context, validator, default priority, and fallback writer.
	logger := NewLoggerWithFallback(lctx, "MyApp", fallbackWriter)

	// Log a message for each iteration of the benchmark
	b.ResetTimer() // Reset the timer to ignore setup time
	for i := 0; i < b.N; i++ {
		// Add the iteration number to the log data
		data := map[string]interface{}{
			"username":  "john",
			"iteration": i,
		}

		logger.LogActivity("User logged in", data)
	}
	b.StopTimer() // Stop the timer to ignore cleanup time

	// Close the Kafka writer when done
	if err := kw.Close(); err != nil {
		b.Fatalf("failed to close Kafka writer: %v", err)
	}
}

func BenchmarkParallelLogWriting(b *testing.B) {
	// Create a Kafka connection pool

	// Initialize Kafka connection pool
	pool, err := NewKafkaConnectionPool(10, KafkaConfig{ // Assuming max 10 connections
		Brokers: []string{"localhost:9092"},
	})
	if err != nil {
		b.Fatalf("unable to create Kafka connection pool: %v", err)
	}

	topic := "log_topic"

	kw := NewKafkaWriter(pool, topic)

	// Create a fallback writer that uses stdout as the fallback.
	fallbackWriter := NewFallbackWriter(kw, os.Stdout)
	// Create a logger context with the default priority.
	lctx := NewLoggerContext(Info)

	var wg sync.WaitGroup

	b.ResetTimer() // Reset the timer to ignore setup time

	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Initialize the logger with the context, validator, default priority, and fallback writer.
			logger := NewLoggerWithFallback(lctx, fmt.Sprintf("MyApp-%d", id), fallbackWriter)

			// Log a message
			data := map[string]interface{}{
				"username":  "john",
				"iteration": id,
			}

			logger.LogActivity("User logged in", data)
		}(i)
	}

	wg.Wait()

	b.StopTimer() // Stop the timer to ignore cleanup time

	// Close the Kafka writer when done
	if err := kw.Close(); err != nil {
		b.Fatalf("failed to close Kafka writer: %v", err)
	}
}

func BenchmarkParallelLogWriting2(b *testing.B) {
	// Initialize Kafka connection pool
	pool, err := NewKafkaConnectionPool(10, KafkaConfig{
		Brokers: []string{"localhost:9092"},
	})
	if err != nil {
		b.Fatalf("unable to create Kafka connection pool: %v", err)
	}

	// Create a Kafka writer with the pool and topic
	topic := "log_topic"
	kw := NewKafkaWriter(pool, topic)

	// Create a fallback writer that uses stdout as the fallback.
	fallbackWriter := NewFallbackWriter(kw, os.Stdout)

	// Create a logger context with the default priority.
	lctx := NewLoggerContext(Info)

	b.ResetTimer() // Reset the timer to ignore setup time

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Initialize the logger with the context, appName, and fallback writer.
			appName := "MyApp"
			logger := NewLoggerWithFallback(lctx, appName, fallbackWriter)

			// Log a message
			data := map[string]interface{}{
				"username": "john",
			}
			logger.LogActivity("User logged in", data)
		}
	})

	b.StopTimer() // Stop the timer to ignore cleanup time

	// Close the Kafka writer when done
	if err := kw.Close(); err != nil {
		b.Fatalf("failed to close Kafka writer: %v", err)
	}
}
