package logharbour

import (
	"time"

	"github.com/IBM/sarama"
)

type KafkaConfig struct {
	Brokers []string // List of broker addresses
	Topic   string   // Kafka topic to write messages to

	// Producer configurations
	Retries          *int                 // Maximum number of times to retry sending a message
	RequiredAcks     *sarama.RequiredAcks // Number of acknowledgments required before considering a message as sent
	Timeout          *time.Duration       // Maximum duration to wait for the broker to acknowledge the receipt of a message
	ReturnErrors     *bool                // Whether to return errors that occurred while producing the message
	ReturnSuccesses  *bool                // Whether to return successes of produced messages
	CompressionLevel *int                 // Compression level to use for messages

	// Network configurations
	DialTimeout     *time.Duration // Timeout for establishing network connections
	ReadTimeout     *time.Duration // Timeout for network reads
	WriteTimeout    *time.Duration // Timeout for network writes
	MaxOpenRequests *int           // Maximum number of unacknowledged requests to send before blocking

	// Client configurations
	ClientID *string // User-provided string sent with every request for logging, debugging, and auditing purposes
}

type KafkaWriter interface {
	Write(p []byte) (n int, err error)
	Close() error
}

type kafkaWriter struct {
	pool  *KafkaConnectionPool
	topic string
}

func (kw *kafkaWriter) Write(p []byte) (n int, err error) {
	producer := kw.pool.GetConnection()
	defer kw.pool.ReleaseConnection(producer)

	msg := &sarama.ProducerMessage{
		Topic: kw.topic,
		Value: sarama.ByteEncoder(p),
	}

	_, _, err = producer.SendMessage(msg)
	if err != nil {
		return 0, err
	}

	return len(p), nil
}

func (kw *kafkaWriter) Close() error {
	for i := 0; i < kw.pool.maxConnections; i++ {
		producer := kw.pool.GetConnection()
		if err := producer.Close(); err != nil {
			return err
		}
	}
	return nil
}

///// pool

type KafkaConnectionPool struct {
	connections    chan sarama.SyncProducer
	maxConnections int
}

func NewKafkaConnectionPool(maxConnections int, kafkaConfig KafkaConfig) (*KafkaConnectionPool, error) {
	pool := &KafkaConnectionPool{
		connections:    make(chan sarama.SyncProducer, maxConnections),
		maxConnections: maxConnections,
	}

	saramaConfig := ApplyKafkaConfig(kafkaConfig)

	for i := 0; i < maxConnections; i++ {
		producer, err := sarama.NewSyncProducer(kafkaConfig.Brokers, saramaConfig)
		if err != nil {
			return nil, err
		}
		pool.connections <- producer
	}

	return pool, nil
}

func NewKafkaWriter(pool *KafkaConnectionPool, topic string) KafkaWriter {
	return &kafkaWriter{
		pool:  pool,
		topic: topic,
	}
}

func (pool *KafkaConnectionPool) GetConnection() sarama.SyncProducer {
	return <-pool.connections
}

func (pool *KafkaConnectionPool) ReleaseConnection(producer sarama.SyncProducer) {
	pool.connections <- producer
}

func ApplyKafkaConfig(kafkaConfig KafkaConfig) *sarama.Config {
	saramaConfig := sarama.NewConfig()

	if kafkaConfig.Retries != nil {
		saramaConfig.Producer.Retry.Max = *kafkaConfig.Retries
	}

	if kafkaConfig.RequiredAcks != nil {
		saramaConfig.Producer.RequiredAcks = *kafkaConfig.RequiredAcks
	}

	if kafkaConfig.Timeout != nil {
		saramaConfig.Producer.Timeout = *kafkaConfig.Timeout
	}

	if kafkaConfig.ReturnErrors != nil {
		saramaConfig.Producer.Return.Errors = *kafkaConfig.ReturnErrors
	}

	if kafkaConfig.ReturnSuccesses != nil {
		saramaConfig.Producer.Return.Successes = *kafkaConfig.ReturnSuccesses
	}

	if kafkaConfig.CompressionLevel != nil {
		saramaConfig.Producer.CompressionLevel = *kafkaConfig.CompressionLevel
	}

	if kafkaConfig.DialTimeout != nil {
		saramaConfig.Net.DialTimeout = *kafkaConfig.DialTimeout
	}

	if kafkaConfig.ReadTimeout != nil {
		saramaConfig.Net.ReadTimeout = *kafkaConfig.ReadTimeout
	}

	if kafkaConfig.WriteTimeout != nil {
		saramaConfig.Net.WriteTimeout = *kafkaConfig.WriteTimeout
	}

	if kafkaConfig.MaxOpenRequests != nil {
		saramaConfig.Net.MaxOpenRequests = *kafkaConfig.MaxOpenRequests
	}

	if kafkaConfig.ClientID != nil {
		saramaConfig.ClientID = *kafkaConfig.ClientID
	}

	return saramaConfig
}
