package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/RichardKnop/machinery/v1/brokers/errs"
	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"

	"github.com/Shopify/sarama"
)

type taskConsumerGroupHandler struct {
	broker        *broker
	taskProcessor iface.TaskProcessor
}

func (taskConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (taskConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h taskConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		// Unmarshal message body into signature struct
		signature := new(tasks.Signature)
		decoder := json.NewDecoder(bytes.NewReader(msg.Value))
		decoder.UseNumber()
		if err := decoder.Decode(signature); err != nil {
			return errs.NewErrCouldNotUnmarshaTaskSignature(msg.Value, err)
		}

		// If the task is not registered, we nack it and requeue,
		// there might be different workers for processing specific tasks
		if !h.broker.IsTaskRegistered(signature.Name) {
			log.INFO.Printf("Task not registered with this worker: %s", msg.Value)
			return fmt.Errorf("Task not registered with this worker: %s", signature.Name)
		}

		log.INFO.Printf("Received new message: %s", msg.Value)

		err := h.taskProcessor.Process(signature)
		if err != nil {
			return err
		}

		sess.MarkMessage(msg, "")
	}
	return nil
}

// broker represents a Kafka broker
type broker struct {
	common.Broker
	consumerGroup sarama.ConsumerGroup
}

// New creates new Broker instance
func New(cnf *config.Config) iface.Broker {
	return &broker{
		Broker: common.NewBroker(cnf),
	}
}

func (b *broker) StartConsuming(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) (bool, error) {
	b.Broker.StartConsuming(consumerTag, concurrency, taskProcessor)

	// Init config, specify appropriate version
	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0

	// Compute the broker address
	broker, err := brokerAddress(b.GetConfig().Broker)
	if err != nil {
		// It is useless to retry, considering that the URL is invalid
		return false, err
	}

	// Start a new consumer group
	b.consumerGroup, err = sarama.NewConsumerGroup([]string{broker}, b.GetConfig().Kafka.ConsumerGroup, config)
	if err != nil {
		b.GetRetryFunc()(b.GetRetryStopChan())
		return b.GetRetry(), err
	}
	defer b.consumerGroup.Close()

	handler := taskConsumerGroupHandler{
		broker:        b,
		taskProcessor: taskProcessor,
	}

	// Start the consuming function.
	for {
		topics := []string{b.GetConfig().Kafka.Topic}

		err := b.consumerGroup.Consume(context.Background(), topics, handler)
		if err != nil {
			return b.GetRetry(), err
		}
	}
}

func (b *broker) StopConsuming() {
	b.Broker.StopConsuming()
}

func (b *broker) Publish(ctx context.Context, task *tasks.Signature) error {
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

	broker, err := brokerAddress(b.GetConfig().Broker)
	if err != nil {
		return fmt.Errorf("Invalid broker URL: %s", err)
	}

	producer, err := sarama.NewSyncProducer([]string{broker}, nil)
	if err != nil {
		return fmt.Errorf("Error creating the producer: %s", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.FATAL.Fatal(err)
		}
	}()

	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(value)}
	_, _, err = producer.SendMessage(msg)

	return err
}

func brokerAddress(uri string) (string, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", err
	}

	addr := u.Hostname()
	port := u.Port()

	if port != "" {
		addr += ":" + port
	}
	return addr, nil
}
