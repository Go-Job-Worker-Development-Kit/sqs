package internal

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/request"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/go-jwdk/jobworker"
)

func TestNewSubscription(t *testing.T) {
	type args struct {
		queueAttributes *QueueAttributes
		raw             *sqs.SQS
		conn            jobworker.Connector
		meta            map[string]string
	}
	tests := []struct {
		name string
		args args
		want *Subscription
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewSubscription(tt.args.queueAttributes, tt.args.raw, tt.args.conn, tt.args.meta); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewSubscription() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_extractMetadata(t *testing.T) {
	type args struct {
		meta map[string]string
	}
	tests := []struct {
		name                    string
		args                    args
		wantPollingInterval     time.Duration
		wantVisibilityTimeout   *int64
		wantWaitTimeSeconds     *int64
		wantMaxNumberOfMessages *int64
	}{
		{
			name: "normal case",
			args: args{
				meta: map[string]string{
					"PollingInterval":   "1",
					"VisibilityTimeout": "2",
					"WaitTimeSeconds":   "3",
					"MaxNumberOfJobs":   "4",
				},
			},
			wantPollingInterval:     1 * time.Second,
			wantVisibilityTimeout:   aws.Int64(2),
			wantWaitTimeSeconds:     aws.Int64(3),
			wantMaxNumberOfMessages: aws.Int64(4),
		},
		{
			name: "should apply default value",
			args: args{
				meta: map[string]string{
					"VisibilityTimeout": "2",
					"WaitTimeSeconds":   "3",
					"MaxNumberOfJobs":   "4",
				},
			},
			wantPollingInterval:     3 * time.Second,
			wantVisibilityTimeout:   aws.Int64(2),
			wantWaitTimeSeconds:     aws.Int64(3),
			wantMaxNumberOfMessages: aws.Int64(4),
		},
		{
			name: "should apply default value",
			args: args{
				meta: map[string]string{
					"PollingInterval":   "InvalidValue",
					"VisibilityTimeout": "2",
					"WaitTimeSeconds":   "3",
					"MaxNumberOfJobs":   "4",
				},
			},
			wantPollingInterval:     3 * time.Second,
			wantVisibilityTimeout:   aws.Int64(2),
			wantWaitTimeSeconds:     aws.Int64(3),
			wantMaxNumberOfMessages: aws.Int64(4),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPollingInterval, gotVisibilityTimeout, gotWaitTimeSeconds, gotMaxNumberOfMessages := extractMetadata(tt.args.meta)
			if gotPollingInterval != tt.wantPollingInterval {
				t.Errorf("extractMetadata() gotPollingInterval = %v, want %v", gotPollingInterval, tt.wantPollingInterval)
			}
			if *gotVisibilityTimeout != *tt.wantVisibilityTimeout {
				t.Errorf("extractMetadata() gotVisibilityTimeout = %v, want %v", gotVisibilityTimeout, tt.wantVisibilityTimeout)
			}
			if *gotWaitTimeSeconds != *tt.wantWaitTimeSeconds {
				t.Errorf("extractMetadata() gotWaitTimeSeconds = %v, want %v", gotWaitTimeSeconds, tt.wantWaitTimeSeconds)
			}
			if *gotMaxNumberOfMessages != *tt.wantMaxNumberOfMessages {
				t.Errorf("extractMetadata() gotMaxNumberOfMessages = %v, want %v", gotMaxNumberOfMessages, tt.wantMaxNumberOfMessages)
			}
		})
	}
}

func TestSubscription_Active(t *testing.T) {
	type fields struct {
		state int32
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "active",
			fields: fields{
				state: subStateActive,
			},
			want: true,
		},
		{
			name: "no active",
			fields: fields{
				state: subStateClosing,
			},
			want: false,
		},
		{
			name: "no active",
			fields: fields{
				state: subStateClosed,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Subscription{
				state: tt.fields.state,
			}
			if got := s.Active(); got != tt.want {
				t.Errorf("Subscription.Active() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSubscription_UnSubscribe(t *testing.T) {
	type fields struct {
		state int32
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "success",
			fields: fields{
				state: subStateActive,
			},
			wantErr: false,
		},
		{
			name: "fail",
			fields: fields{
				state: subStateClosing,
			},
			wantErr: true,
		},
		{
			name: "fail",
			fields: fields{
				state: subStateClosed,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Subscription{
				state: tt.fields.state,
			}
			if err := s.UnSubscribe(); (err != nil) != tt.wantErr {
				t.Errorf("Subscription.UnSubscribe() error = %v, wantClosedChan %v", err, tt.wantErr)
			}
		})
	}
}

func TestSubscription_writeMessageChan(t *testing.T) {
	type fields struct {
		queueAttributes     *QueueAttributes
		pollingInterval     time.Duration
		visibilityTimeout   *int64
		waitTimeSeconds     *int64
		maxNumberOfMessages *int64
		receiveMessage      func(ctx aws.Context, input *sqs.ReceiveMessageInput, opts ...request.Option) (*sqs.ReceiveMessageOutput, error)
		state               int32
	}
	type args struct {
		ch chan *sqs.Message
	}
	tests := []struct {
		name            string
		fields          fields
		args            args
		wantMessageSize int
		wantClosedChan  bool
	}{
		{
			name: "normal case",
			fields: fields{
				queueAttributes: &QueueAttributes{URL: "http://localhost/test"},
				receiveMessage: func(ctx aws.Context, input *sqs.ReceiveMessageInput, opts ...request.Option) (output *sqs.ReceiveMessageOutput, e error) {
					time.Sleep(time.Second / 3)
					return &sqs.ReceiveMessageOutput{
						Messages: []*sqs.Message{
							{}, {}, {},
						},
					}, nil
				},
				state: subStateActive,
			},
			args: args{
				ch: make(chan *sqs.Message, 10),
			},
			wantMessageSize: 3,
			wantClosedChan:  false,
		},
		{
			name: "state closing",
			fields: fields{
				state: subStateClosing,
			},
			args: args{
				ch: make(chan *sqs.Message, 10),
			},
			wantMessageSize: 0,
			wantClosedChan:  true,
		},
		{
			name: "state closed",
			fields: fields{
				state: subStateClosed,
			},
			args: args{
				ch: make(chan *sqs.Message, 10),
			},
			wantMessageSize: 0,
			wantClosedChan:  true,
		},
		{
			name: "receiveMessage func return error",
			fields: fields{
				queueAttributes: &QueueAttributes{URL: "http://localhost/test"},
				receiveMessage: func(ctx aws.Context, input *sqs.ReceiveMessageInput, opts ...request.Option) (output *sqs.ReceiveMessageOutput, e error) {
					time.Sleep(time.Second / 3)
					return nil, errors.New("test")
				},
				state: subStateActive,
			},
			args: args{
				ch: make(chan *sqs.Message, 10),
			},
			wantMessageSize: 0,
			wantClosedChan:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Subscription{
				queueAttributes:     tt.fields.queueAttributes,
				pollingInterval:     tt.fields.pollingInterval,
				visibilityTimeout:   tt.fields.visibilityTimeout,
				waitTimeSeconds:     tt.fields.waitTimeSeconds,
				maxNumberOfMessages: tt.fields.maxNumberOfMessages,
				receiveMessage:      tt.fields.receiveMessage,
				state:               tt.fields.state,
			}
			go s.writeMessageChan(tt.args.ch)
			time.Sleep(time.Second / 2)
			if tt.wantClosedChan {
				if _, ok := <-tt.args.ch; ok {
					t.Errorf("Subscription.writeMessageChan() result = %v, wantClosedChan %v", ok, tt.wantClosedChan)
				}
			}
			if len(tt.args.ch) != tt.wantMessageSize {
				t.Errorf("Subscription.writeMessageChan() size = %v, wantSize %v", len(tt.args.ch), tt.wantMessageSize)
			}
		})
	}
}

func TestSubscription_readMessageChan(t *testing.T) {
	type fields struct {
		queueAttributes     *QueueAttributes
		raw                 *sqs.SQS
		conn                jobworker.Connector
		pollingInterval     time.Duration
		visibilityTimeout   *int64
		waitTimeSeconds     *int64
		maxNumberOfMessages *int64
		queue               chan *jobworker.Job
		state               int32
	}
	type args struct {
		ch chan *sqs.Message
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Subscription{
				queueAttributes:     tt.fields.queueAttributes,
				raw:                 tt.fields.raw,
				conn:                tt.fields.conn,
				pollingInterval:     tt.fields.pollingInterval,
				visibilityTimeout:   tt.fields.visibilityTimeout,
				waitTimeSeconds:     tt.fields.waitTimeSeconds,
				maxNumberOfMessages: tt.fields.maxNumberOfMessages,
				queue:               tt.fields.queue,
				state:               tt.fields.state,
			}
			s.readMessageChan(tt.args.ch)
		})
	}
}

func TestSubscription_closeQueue(t *testing.T) {
	type fields struct {
		queueAttributes     *QueueAttributes
		raw                 *sqs.SQS
		conn                jobworker.Connector
		pollingInterval     time.Duration
		visibilityTimeout   *int64
		waitTimeSeconds     *int64
		maxNumberOfMessages *int64
		queue               chan *jobworker.Job
		state               int32
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Subscription{
				queueAttributes:     tt.fields.queueAttributes,
				raw:                 tt.fields.raw,
				conn:                tt.fields.conn,
				pollingInterval:     tt.fields.pollingInterval,
				visibilityTimeout:   tt.fields.visibilityTimeout,
				waitTimeSeconds:     tt.fields.waitTimeSeconds,
				maxNumberOfMessages: tt.fields.maxNumberOfMessages,
				queue:               tt.fields.queue,
				state:               tt.fields.state,
			}
			s.closeQueue()
		})
	}
}
