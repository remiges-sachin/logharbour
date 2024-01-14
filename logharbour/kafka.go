package logharbour

import (
	"github.com/IBM/sarama"
)

type KafkaConfig struct {
	Brokers []string
	Topic   string
	// Include other Kafka producer configurations as needed
}

type KafkaWriter interface {
	Write(p []byte) (n int, err error)
	Flush() error
	Close() error

	// Errors() <-chan error
	Configure(config KafkaConfig) error
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

func (sw *kafkaWriter) Flush() error {
	// Sarama does not have a Flush method, as it sends messages as soon as they are available.
	return nil
}

func (sw *kafkaWriter) Configure(config KafkaConfig) error {
	// Implement this method based on your requirements.
	return nil
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

func NewKafkaConnectionPool(maxConnections int, config KafkaConfig) (*KafkaConnectionPool, error) {
	pool := &KafkaConnectionPool{
		connections:    make(chan sarama.SyncProducer, maxConnections),
		maxConnections: maxConnections,
	}

	for i := 0; i < maxConnections; i++ {
		producer, err := sarama.NewSyncProducer(config.Brokers, nil)
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
