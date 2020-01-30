package internal

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/go-job-worker-development-kit/jobworker"
)

const (
	subStateActive  = 0
	subStateClosing = 1
	subStateClosed  = 2
)

func NewSubscription(name string, raw *sqs.SQS) *Subscription {
	return &Subscription{
		name:  name,
		raw:   raw,
		queue: make(chan *jobworker.Job),
	}
}

type Subscription struct {
	name  string
	raw   *sqs.SQS
	queue chan *jobworker.Job
	state int32
}

func (s *Subscription) Active() bool {
	return atomic.LoadInt32(&s.state) == subStateActive
}

func (s *Subscription) Queue() chan *jobworker.Job {
	return s.queue
}

var ErrCompletedSubscription = errors.New("subscription is unsubscribed")

func (s *Subscription) UnSubscribe() error {
	if !atomic.CompareAndSwapInt32(&s.state, subStateActive, subStateClosing) {
		return ErrCompletedSubscription
	}
	return nil
}

func (s *Subscription) ReadLoop(conn jobworker.Connector) {

	ch := make(chan *sqs.Message)

	go func() {
		for {
			result, err := s.raw.ReceiveMessageWithContext(context.Background(), &sqs.ReceiveMessageInput{
				AttributeNames: []*string{
					aws.String(sqs.QueueAttributeNameAll),
				},
				MessageAttributeNames: []*string{
					aws.String(sqs.QueueAttributeNameAll),
				},
				QueueUrl: aws.String(s.name),
			})
			if err != nil {
				close(ch)
				// TODO logging
				return
			}
			if len(result.Messages) == 0 {
				time.Sleep(time.Second) // TODO to params
				continue
			}
			for _, msg := range result.Messages {
				ch <- msg
			}
		}
	}()

	for {
		msg, ok := <-ch
		if !ok {
			state := atomic.LoadInt32(&s.state)
			if state == subStateActive || state == subStateClosing {
				s.closeQueue()
			}
			return
		}

		for k, v := range msg.Attributes {
			sentTimestamp := msg.Attributes[sqs.MessageSystemAttributeNameSentTimestamp]
			approximateReceiveCount := msg.Attributes[sqs.MessageSystemAttributeNameApproximateReceiveCount]
			messageDeduplicationId := msg.Attributes[sqs.MessageSystemAttributeNameMessageDeduplicationId]
			messageGroupId := msg.Attributes[sqs.MessageSystemAttributeNameMessageGroupId]
		}

		class := msg.MessageAttributes[messageAttributeNameJobClass]

		retryCount, err := strconv.ParseInt(aws.StringValue(approximateReceiveCount), 10, 64)
		if err != nil {
			c.debug("could not parse approximateReceiveCount:", approximateReceiveCount, err)
		}

		enqueueAt, err := strconv.ParseInt(aws.StringValue(sentTimestamp), 10, 64)
		if err != nil {
			c.debug("could not parse sentTimestamp:", sentTimestamp, err)
		}

		job := &internal.Job{
			JobID:           aws.StringValue(msg.MessageId),
			Class:           aws.StringValue(class.StringValue),
			ReceiptID:       aws.StringValue(msg.ReceiptHandle),
			Args:            aws.StringValue(msg.Body),
			DeduplicationID: aws.StringValue(messageDeduplicationId),
			GroupID:         aws.StringValue(messageGroupId),
			RetryCount:      retryCount,
			EnqueueAt:       enqueueAt,
		}

		s.queue <- newJob(s.name, msg, conn)
	}
}

func (s *Subscription) closeQueue() {
	atomic.StoreInt32(&s.state, subStateClosed)
	close(s.queue)
}

func newJob(queue string, msg *sqs.Message, conn jobworker.Connector) *jobworker.Job {
	metadata := make(map[string]string)
	for k, v := range msg.Attributes {
		if v != nil {
			metadata[k] = *v
		}
	}
	for k, v := range msg.MessageAttributes {
		if v.DataType != nil {
			switch v.DataType {
			case :
				
			}
			metadata[k] = *
		}
	}
	job := jobworker.NewJob(
		queue,
		aws.StringValue(msg.Body),
		metadata,
		conn,
		msg,
	)
	return job
}
