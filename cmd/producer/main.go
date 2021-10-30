package main

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

}

//kafka-console-consumer --bootstrap-server=localhost:9092 --topic=teste

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "gokafka_kafka_1:9092",
	}

	p, err :- kafka.NewProducer(configMap)

	if err != nil {
		log.Println(err.Error())
	}
	
	return p
}
