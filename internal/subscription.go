package internal

import (
	"context"
	"errors"
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

func NewSubscription(name string, raw *sqs.SQS, conn jobworker.Connector) *Subscription {
	return &Subscription{
		name:  name,
		raw:   raw,
		conn:  conn,
		queue: make(chan *jobworker.Job),
	}
}

type Subscription struct {
	name  string
	raw   *sqs.SQS
	conn  jobworker.Connector
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
				// TODO logging
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
		s.queue <- newJob(s.name, msg, s.conn)
	}
}

func (s *Subscription) closeQueue() {
	atomic.StoreInt32(&s.state, subStateClosed)
	close(s.queue)
}
