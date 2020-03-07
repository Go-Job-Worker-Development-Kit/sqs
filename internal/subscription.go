package internal

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/go-jwdk/jobworker"
)

const (
	subStateActive  = 0
	subStateClosing = 1
	subStateClosed  = 2
)

func NewSubscription(queueAttributes *QueueAttributes,
	raw *sqs.SQS, conn jobworker.Connector, meta map[string]string) *Subscription {
	pollingInterval, visibilityTimeout, waitTimeSeconds, maxNumberOfMessages := extractMetadata(meta)
	return &Subscription{
		queueAttributes:     queueAttributes,
		raw:                 raw,
		conn:                conn,
		pollingInterval:     pollingInterval,
		visibilityTimeout:   visibilityTimeout,
		waitTimeSeconds:     waitTimeSeconds,
		maxNumberOfMessages: maxNumberOfMessages,
		queue:               make(chan *jobworker.Job),
		state:               subStateActive,
	}
}

func extractMetadata(meta map[string]string) (
	pollingInterval time.Duration,
	visibilityTimeout *int64,
	waitTimeSeconds *int64,
	maxNumberOfMessages *int64,
) {

	if v := meta["PollingInterval"]; v != "" {
		i, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			pollingInterval = time.Duration(i) * time.Second
		}
	}

	if v := meta["VisibilityTimeout"]; v != "" {
		i, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			visibilityTimeout = &i
		}
	}

	if v := meta["WaitTimeSeconds"]; v != "" {
		i, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			waitTimeSeconds = &i
		}
	}

	if v := meta["MaxNumberOfJobs"]; v != "" {
		i, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			maxNumberOfMessages = &i
		}
	}
	return
}

type Subscription struct {
	queueAttributes *QueueAttributes
	raw             *sqs.SQS
	conn            jobworker.Connector

	pollingInterval     time.Duration
	visibilityTimeout   *int64
	waitTimeSeconds     *int64
	maxNumberOfMessages *int64

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

func (s *Subscription) ReadLoop() {

	ch := make(chan *sqs.Message)

	go func() {
		for {

			if atomic.LoadInt32(&s.state) != subStateActive {
				close(ch)
				return
			}

			ctx := context.Background()
			result, err := s.raw.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
				AttributeNames: []*string{
					aws.String(sqs.QueueAttributeNameAll),
				},
				MessageAttributeNames: []*string{
					aws.String(sqs.QueueAttributeNameAll),
				},
				QueueUrl: aws.String(s.queueAttributes.URL),

				VisibilityTimeout:   s.visibilityTimeout,
				WaitTimeSeconds:     s.waitTimeSeconds,
				MaxNumberOfMessages: s.maxNumberOfMessages,
			})
			if err != nil {
				close(ch)
				return
			}
			if len(result.Messages) == 0 {
				time.Sleep(s.pollingInterval)
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
		s.queue <- newJob(s.queueAttributes.Name, msg, s.conn)
	}
}

func (s *Subscription) closeQueue() {
	atomic.StoreInt32(&s.state, subStateClosed)
	close(s.queue)
}
