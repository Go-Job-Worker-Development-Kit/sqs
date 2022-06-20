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
		{
			name: "normal case",
			args: args{
				queueAttributes: &QueueAttributes{URL: "http://localhost/test"},
				meta: map[string]string{
					"PollingInterval":   "1",
					"VisibilityTimeout": "2",
					"WaitTimeSeconds":   "3",
					"MaxNumberOfJobs":   "4",
				},
			},
			want: &Subscription{
				queueAttributes:     &QueueAttributes{URL: "http://localhost/test"},
				pollingInterval:     time.Second,
				visibilityTimeout:   aws.Int64(2),
				waitTimeSeconds:     aws.Int64(3),
				maxNumberOfMessages: aws.Int64(4),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewSubscription(tt.args.queueAttributes, tt.args.raw, tt.args.conn, tt.args.meta)
			if !reflect.DeepEqual(got.queueAttributes.URL, tt.want.queueAttributes.URL) {
				t.Errorf("NewSubscription() = %v, want %v", got.queueAttributes.URL, tt.want.queueAttributes.URL)
			}
			if !reflect.DeepEqual(got.pollingInterval, tt.want.pollingInterval) {
				t.Errorf("NewSubscription() = %v, want %v", got.pollingInterval, tt.want.pollingInterval)
			}
			if !reflect.DeepEqual(got.visibilityTimeout, tt.want.visibilityTimeout) {
				t.Errorf("NewSubscription() = %v, want %v", got.visibilityTimeout, tt.want.visibilityTimeout)
			}
			if !reflect.DeepEqual(got.waitTimeSeconds, tt.want.waitTimeSeconds) {
				t.Errorf("NewSubscription() = %v, want %v", got.waitTimeSeconds, tt.want.waitTimeSeconds)
			}
			if !reflect.DeepEqual(got.maxNumberOfMessages, tt.want.maxNumberOfMessages) {
				t.Errorf("NewSubscription() = %v, want %v", got.maxNumberOfMessages, tt.want.maxNumberOfMessages)
			}
			if reflect.DeepEqual(got.receiveMessage, nil) {
				t.Errorf("NewSubscription() = func, want %v", "a not nil")
			}
			if reflect.DeepEqual(got.queue, nil) {
				t.Errorf("NewSubscription() = %v, want %v", got.queue, "a not nil")
			}
			if !reflect.DeepEqual(got.state, subStateActive) {
				t.Errorf("NewSubscription() = %v, want %v", got.state, subStateActive)
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
		wantVisibilityTimeout   *int64
		wantWaitTimeSeconds     *int64
		wantMaxNumberOfMessages *int64
		wantNil                 bool
	}{
		{
			name: "normal case",
			args: args{
				meta: map[string]string{
					"VisibilityTimeout": "2",
					"WaitTimeSeconds":   "3",
					"MaxNumberOfJobs":   "4",
				},
			},
			wantVisibilityTimeout:   aws.Int64(2),
			wantWaitTimeSeconds:     aws.Int64(3),
			wantMaxNumberOfMessages: aws.Int64(4),
		},
		{
			name: "should apply nil",
			args: args{
				meta: map[string]string{},
			},
			wantNil: true,
		},
		{
			name: "should apply nil",
			args: args{
				meta: map[string]string{
					"VisibilityTimeout": "InvalidValue",
					"WaitTimeSeconds":   "InvalidValue",
					"MaxNumberOfJobs":   "InvalidValue",
				},
			},
			wantNil: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, gotVisibilityTimeout, gotWaitTimeSeconds, gotMaxNumberOfMessages := extractMetadata(tt.args.meta)
			if tt.wantNil {
				if gotVisibilityTimeout != nil {
					t.Errorf("extractMetadata() gotVisibilityTimeout = %v, want %v", gotVisibilityTimeout, tt.wantVisibilityTimeout)
				}
				if gotWaitTimeSeconds != nil {
					t.Errorf("extractMetadata() gotWaitTimeSeconds = %v, want %v", gotWaitTimeSeconds, tt.wantWaitTimeSeconds)
				}
				if gotMaxNumberOfMessages != nil {
					t.Errorf("extractMetadata() gotMaxNumberOfMessages = %v, want %v", gotMaxNumberOfMessages, tt.wantMaxNumberOfMessages)
				}
				return
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

func Test_extractMetadata_PollingInterval(t *testing.T) {
	type args struct {
		meta map[string]string
	}
	tests := []struct {
		name                string
		args                args
		wantPollingInterval time.Duration
	}{
		{
			name: "normal case",
			args: args{
				meta: map[string]string{
					"PollingInterval": "1",
				},
			},
			wantPollingInterval: 1 * time.Second,
		},
		{
			name: "should apply default value",
			args: args{
				meta: map[string]string{},
			},
			wantPollingInterval: 3 * time.Second,
		},
		{
			name: "should apply default value",
			args: args{
				meta: map[string]string{
					"PollingInterval": "InvalidValue",
				},
			},
			wantPollingInterval: 3 * time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPollingInterval, _, _, _ := extractMetadata(tt.args.meta)
			if gotPollingInterval != tt.wantPollingInterval {
				t.Errorf("extractMetadata() gotPollingInterval = %v, want %v", gotPollingInterval, tt.wantPollingInterval)
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
			wantClosedChan:  false,
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
			defer s.UnSubscribe()
			go s.writeMessageChan(tt.args.ch)
			time.Sleep(time.Second / 2)
			if tt.wantClosedChan {
				if _, ok := <-tt.args.ch; ok {
					t.Errorf("Subscription.writeMessageChan() result = %v, wantClosedChan %v", ok, tt.wantClosedChan)
				}
				return
			}
			if len(tt.args.ch) != tt.wantMessageSize {
				t.Errorf("Subscription.writeMessageChan() size = %v, wantSize %v", len(tt.args.ch), tt.wantMessageSize)
			}
		})
	}
}

func TestSubscription_readMessageChan(t *testing.T) {
	type fields struct {
		queueAttributes *QueueAttributes
		queue           chan *jobworker.Job
		state           int32
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
				queue:           make(chan *jobworker.Job),
				state:           subStateActive,
			},
			args: args{
				ch: make(chan *sqs.Message, 10),
			},
			wantMessageSize: 1,
			wantClosedChan:  false,
		},
		{
			name: "closed message chan and state is active",
			fields: fields{
				queueAttributes: &QueueAttributes{URL: "http://localhost/test"},
				queue:           make(chan *jobworker.Job),
				state:           subStateActive,
			},
			args: args{
				ch: make(chan *sqs.Message, 10),
			},
			wantMessageSize: 1,
			wantClosedChan:  true,
		},
		{
			name: "closed message chan and state is closing",
			fields: fields{
				queueAttributes: &QueueAttributes{URL: "http://localhost/test"},
				queue:           make(chan *jobworker.Job),
				state:           subStateClosing,
			},
			args: args{
				ch: make(chan *sqs.Message, 10),
			},
			wantMessageSize: 1,
			wantClosedChan:  true,
		},
		{
			name: "closed message chan and state is closed",
			fields: fields{
				queueAttributes: &QueueAttributes{URL: "http://localhost/test"},
				queue: func() chan *jobworker.Job {
					ch := make(chan *jobworker.Job)
					close(ch)
					return ch
				}(),
				state: subStateClosed,
			},
			args: args{
				ch: make(chan *sqs.Message, 10),
			},
			wantMessageSize: 1,
			wantClosedChan:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Subscription{
				queueAttributes: tt.fields.queueAttributes,
				queue:           tt.fields.queue,
				state:           tt.fields.state,
			}

			go s.readMessageChan(tt.args.ch)
			if tt.wantClosedChan {
				close(tt.args.ch)
				if _, ok := <-s.queue; ok {
					t.Errorf("Subscription.readMessageChan() result = %v, wantClosedChan %v", ok, tt.wantClosedChan)
				}
				return
			}

			for i := 0; i < tt.wantMessageSize; i++ {
				tt.args.ch <- &sqs.Message{}
			}
			if len(tt.args.ch) != tt.wantMessageSize {
				t.Errorf("Subscription.readMessageChan() size = %v, wantSize %v", len(tt.args.ch), tt.wantMessageSize)
			}
		})
	}
}
