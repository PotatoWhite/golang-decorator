package simeplestruct

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"log"
)

func NewSimpleStruct(brokers []string, topic string) *SimpleStruct {
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		log.Fatalf("Kafka producer 초기화 실패: %s", err)
	}

	return &SimpleStruct{producer: producer, topic: topic}
}

type SimpleStruct struct {
	producer sarama.SyncProducer
	topic    string
}

func (m *SimpleStruct) test1(a int, b string, c any) (any, error) {
	return fmt.Sprintf("Test1 result: %d %s %v", a, b, c), nil
}

func (m *SimpleStruct) test2(z any, a []int, b string, c any) (int, any, error) {
	return len(b), fmt.Sprintf("Test2 result: %v %v %s %v", z, a, b, c), nil
}

func (m *SimpleStruct) Test1AndPub(a int, b string, c any) (any, error) {
	result, err := m.test1(a, b, c)
	if err == nil {
		err = pub(m.producer, m.topic, "Test1", result)
	}
	return result, err
}

func (m *SimpleStruct) Test2AndPub(z any, a []int, b string, c any) (int, any, error) {
	result1, result2, err := m.test2(z, a, b, c)
	if err == nil {
		err = pub(m.producer, m.topic, "Test2", []any{result1, result2})
	}
	return result1, result2, err
}

func pub(producer sarama.SyncProducer, topic string, key string, data any) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return producer.SendMessages([]*sarama.ProducerMessage{{Topic: topic, Key: sarama.ByteEncoder(key), Value: sarama.ByteEncoder(jsonData)}})
}
