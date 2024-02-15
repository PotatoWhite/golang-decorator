package main

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"log"
)

type MyStruct struct {
	producer sarama.SyncProducer
	topic    string
}

func (m *MyStruct) test1(a int, b string, c any) (any, error) {
	return fmt.Sprintf("Test1 result: %d %s %v", a, b, c), nil
}

func (m *MyStruct) test2(z any, a []int, b string, c any) (int, any, error) {
	return len(b), fmt.Sprintf("Test2 result: %v %v %s %v", z, a, b, c), nil
}

func NewMyStruct(brokers []string, topic string) *MyStruct {
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		log.Fatalf("Kafka producer 초기화 실패: %s", err)
	}

	return &MyStruct{producer: producer, topic: topic}
}

func main() {
	m := NewMyStruct([]string{"localhost:9092"}, "myTopic")

	if result, err := m.Test1AndPub(1, "test", "test"); err != nil {
		log.Fatalf("Test1AndPub failed: %s", err)
	} else {
		fmt.Println("Test1AndPub result:", result)
	}

	if result1, result2, err := m.Test2AndPub(1, []int{1, 2, 3}, "test", "test"); err != nil {
		log.Fatalf("Test2AndPub failed: %s", err)
	} else {
		fmt.Println("Test2AndPub results:", result1, result2)
	}
}

func (m *MyStruct) Test1AndPub(a int, b string, c any) (any, error) {
	return deco(m.producer, m.topic, "Test1", func() ([]any, error) {
		result, err := m.test1(a, b, c)
		if err != nil {
			return nil, err
		}
		// 결과를 슬라이스로 포장하여 DecorateFunction에 전달
		return []any{result}, nil
	})
}

func (m *MyStruct) Test2AndPub(z any, a []int, b string, c any) (int, any, error) {
	results, err := deco(m.producer, m.topic, "Test2", func() ([]any, error) {
		result1, result2, err := m.test2(z, a, b, c)
		if err != nil {
			return nil, err
		}
		// Test2의 반환 값을 슬라이스에 포장하여 반환
		return []any{result1, result2}, nil
	})

	if err != nil {
		return 0, nil, err
	}

	// decoration 의 반환 값에서 Test2의 반환 값을 추출하고 타입 어설션
	if len(results) >= 2 {
		result1, ok1 := results[0].(int)
		if !ok1 {
			return 0, nil, fmt.Errorf("type assertion failed for result1")
		}
		return result1, results[1], nil
	}

	return 0, nil, fmt.Errorf("insufficient results returned from Test2AndPub")
}

func deco(producer sarama.SyncProducer, topic string, key string, lambdaFunc func() ([]any, error)) ([]any, error) {
	results, err := lambdaFunc()
	if err != nil {
		return nil, err
	}

	jsonData, err := json.Marshal(results)
	if err != nil {
		return nil, fmt.Errorf("JSON conversion failed: %v", err)
	}

	message := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(jsonData),
	}

	_, _, err = producer.SendMessage(message)
	if err != nil {
		return nil, fmt.Errorf("Failed to publish Kafka message: %v", err)
	}

	return results, nil
}
