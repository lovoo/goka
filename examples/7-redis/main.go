package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	yaml "gopkg.in/yaml.v2"
)

type Config struct {
	Kafka struct {
		Brokers   []string `yaml:"brokers"`
		Group     string   `yaml:"group"`
		Stream    string   `yaml:"stream"`
		Redis     string   `yaml:"redis"`
		Namespace string   `yaml:"namespace"`
	} `yaml:"kafka"`
}

var (
	filename = flag.String("config", "config.yaml", "path to config file")
)

func main() {
	flag.Parse()

	conf, err := readConfig(*filename)
	if err != nil {
		log.Fatal(err)
	}

	// consuming
	go func() {
		err := Consume(new(nopPublisher), conf.Kafka.Brokers, conf.Kafka.Group,
			conf.Kafka.Stream, conf.Kafka.Redis, conf.Kafka.Namespace)
		if err != nil {
			log.Fatal(err)
		}
	}()

	// producing
	producer, err := NewProducer(conf.Kafka.Brokers, conf.Kafka.Stream)
	for {
		event := &Event{
			UserID:    strconv.FormatInt(rand.Int63n(255), 10),
			Timestamp: time.Now().UnixNano(),
		}
		fmt.Printf("emit     ->key: `%v` ->event: `%v`\n", event.UserID, event)
		err = producer.Emit(event.UserID, event)
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(5 * time.Second)
	}
}

func readConfig(filename string) (*Config, error) {
	b, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	conf := new(Config)
	err = yaml.Unmarshal(b, conf)
	return conf, err
}

type nopPublisher struct{}

func (p *nopPublisher) Publish(ctx context.Context, key string, event *Event) error {
	fmt.Printf("published ->key: `%v` ->event: `%v`\n", key, event)
	return nil
}

func (p *nopPublisher) Close() error {
	return nil
}
