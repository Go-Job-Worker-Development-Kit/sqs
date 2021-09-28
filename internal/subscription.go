package internal

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws/request"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/go-jwdk/jobworker"
)

const (
	subStateActive  = int32(0)
	subStateClosing = int32(1)
	subStateClosed  = int32(2)

	subMetadataKeyPollingInterval   = "PollingInterval"
	subMetadataKeyVisibilityTimeout = "VisibilityTimeout"
	subMetadataKeyWaitTimeSeconds   = "WaitTimeSeconds"
	subMetadataKeyMaxNumberOfJobs   = "MaxNumberOfJobs"

	defaultPollingInterval = 3 * time.Second
)

func NewSubscription(queueAttributes *QueueAttributes,
	svc SQSClient, conn jobworker.Connector, meta map[string]string) *Subscription {
	pollingInterval, visibilityTimeout, waitTimeSeconds, maxNumberOfMessages := extractMetadata(meta)
	return &Subscription{
		queueAttributes:     queueAttributes,
		svc:                 svc,
		conn:                conn,
		pollingInterval:     pollingInterval,
		visibilityTimeout:   visibilityTimeout,
		waitTimeSeconds:     waitTimeSeconds,
		maxNumberOfMessages: maxNumberOfMessages,
		receiveMessage:      svc.ReceiveMessageWithContext, // TODO should use svc
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

	pollingInterval = defaultPollingInterval
	if v := meta[subMetadataKeyPollingInterval]; v != "" {
		i, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			pollingInterval = time.Duration(i) * time.Second
		}
	}

	if v := meta[subMetadataKeyVisibilityTimeout]; v != "" {
		i, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			visibilityTimeout = &i
		}
	}

	if v := meta[subMetadataKeyWaitTimeSeconds]; v != "" {
		i, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			waitTimeSeconds = &i
		}
	}

	if v := meta[subMetadataKeyMaxNumberOfJobs]; v != "" {
		i, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			maxNumberOfMessages = &i
		}
	}
	return
}

type Subscription struct {
	queueAttributes *QueueAttributes
	svc             SQSClient
	conn            jobworker.Connector

	pollingInterval     time.Duration
	visibilityTimeout   *int64
	waitTimeSeconds     *int64
	maxNumberOfMessages *int64

	receiveMessage func(ctx aws.Context, input *sqs.ReceiveMessageInput, opts ...request.Option) (*sqs.ReceiveMessageOutput, error)
	queue          chan *jobworker.Job
	state          int32
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

func (s *Subscription) Start() {
	msgCh := make(chan *sqs.Message)
	go s.writeMessageChan(msgCh)
	s.readMessageChan(msgCh)
}

func (s *Subscription) writeMessageChan(ch chan *sqs.Message) {
	for {
		if atomic.LoadInt32(&s.state) != subStateActive {
			close(ch)
			return
		}
		ctx := context.Background()
		result, err := s.receiveMessage(ctx, &sqs.ReceiveMessageInput{
			AttributeNames: []*string{
				aws.String(sqs.QueueAttributeNameAll),
			},
			MessageAttributeNames: []*string{
				aws.String(sqs.QueueAttributeNameAll),
			},
			QueueUrl:            aws.String(s.queueAttributes.URL),
			VisibilityTimeout:   s.visibilityTimeout,
			WaitTimeSeconds:     s.waitTimeSeconds,
			MaxNumberOfMessages: s.maxNumberOfMessages,
		})
		if err != nil {
			fmt.Println("receiveMessage error", err)
			close(ch)
			return
		}
		fmt.Println("receiveMessage success")
		if len(result.Messages) == 0 {
			fmt.Println("receiveMessage empty")
			time.Sleep(s.pollingInterval)
			fmt.Println("receiveMessage retry")
			continue
		}
		for _, msg := range result.Messages {
			fmt.Println("receiveMessage msg", msg.String())
			ch <- msg
		}
	}
}

func (s *Subscription) readMessageChan(ch chan *sqs.Message) {
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
