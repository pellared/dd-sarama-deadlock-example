package main

import (
	"fmt"
	"log"

	"github.com/Shopify/sarama"
	saramatrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/Shopify/sarama"
)

/*
	EXECUTE:

docker network create confluent

docker run --rm -d \
	--name zookeeper \
	--network confluent \
	-p 2181:2181 \
	-e ZOOKEEPER_CLIENT_PORT=2181 \
	confluentinc/cp-zookeeper:5.0.0

docker run --rm -d \
	--name kafka \
	--network confluent \
	-p 9092:9092 \
	-e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
	-e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
	-e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 \
	-e KAFKA_CREATE_TOPICS=gotest:1:1 \
	-e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
	confluentinc/cp-kafka:5.0.0

go run -race .

*/

func main() {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V0_11_0_0 // minimum version that supports headers which are required for tracing

	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, cfg)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	producer = saramatrace.WrapAsyncProducer(cfg, producer)

	msg := &sarama.ProducerMessage{
		Topic: "some-topic",
		Value: sarama.StringEncoder("Hello World"),
	}

	// close the producer
	if err := producer.Close(); err != nil {
		log.Fatalln(err)
	}

	// esnure that WrapAsyncProducer has exited
	if _, ok := <-producer.Successes(); ok {
		log.Fatalln("should not recieve anything")
	}

	log.Println("Sending message", msg)

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("RECOVERED FROM MAIN:", r)
			return
		}
	}()

	sarama.PanicHandler = func(r interface{}) { // sarama has a data race for PanicHandler...
		fmt.Println("RECOVERED IN SARAMA:", r)
	}

	producer.Input() <- msg // hangs forever
	// without instrumentation it panics from two places!
	// my fix ensures it panics only in one place

	log.Println("Message sent", msg)
}
