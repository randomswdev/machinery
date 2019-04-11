package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"

	"github.com/Shopify/sarama"
)

type KafkaConnection struct {
}

// Broker represents a Kafka broker
type Broker struct {
	common.Broker
}

// New creates new Broker instance
func New(cnf *config.Config) iface.Broker {
	return &Broker{
		Broker: common.NewBroker(cnf),
	}
}

func (b *Broker) StartConsuming(consumerTag string, concurrency int, p iface.TaskProcessor) (bool, error) {

}

func (b *Broker) StopConsuming() {

}

func (b *Broker) Publish(ctx context.Context, task *tasks.Signature) error {
	// Adjust routing key (this decides which queue the message will be published to)
	b.AdjustRoutingKey(task)

	value, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	// Set the topic to publish to
	topic := b.GetConfig().Kafka.Topic

	// Check the taks's ETA field, if it is set and it is in the future,
	// delay the task
	if task.ETA != nil {
		now := time.Now().UTC()

		if task.ETA.After(now) {
			// Let's publish the message to the delayed topic. The assumption is that the
			// producers and consumers all have the clock in synch
			topic = b.GetConfig().Kafka.DelayedTopic
		}
	}

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		return fmt.Errorf("Error creating the producer: %s", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(value)}
	_, _, err = producer.SendMessage(msg)

	return err
}
