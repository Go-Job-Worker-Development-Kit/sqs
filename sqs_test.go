package sqs

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/go-jwdk/aws-sqs-connector/internal"
	"github.com/go-jwdk/jobworker"
)

func TestConnector_resolveQueueAttributes(t *testing.T) {
	type fields struct {
		svc internal.SQSClient
	}
	type args struct {
		ctx  context.Context
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *internal.QueueAttributes
		wantErr bool
	}{
		{
			name: "normal case",
			fields: fields{
				svc: &internal.SQSClientMock{
					GetQueueUrlWithContextFunc: func(ctx aws.Context, input *sqs.GetQueueUrlInput, opts ...request.Option) (output *sqs.GetQueueUrlOutput, e error) {
						return &sqs.GetQueueUrlOutput{
							QueueUrl: aws.String("http://localhost/foo"),
						}, nil
					},
					GetQueueAttributesWithContextFunc: func(ctx aws.Context, input *sqs.GetQueueAttributesInput, opts ...request.Option) (output *sqs.GetQueueAttributesOutput, e error) {
						return &sqs.GetQueueAttributesOutput{
							Attributes: map[string]*string{
								"ABC": aws.String("abc"),
								"DEF": aws.String("def"),
							},
						}, nil
					},
				},
			},
			args: args{
				ctx:  context.Background(),
				name: "foo",
			},
			want: &internal.QueueAttributes{
				Name: "foo",
				URL:  "http://localhost/foo",
				RawAttributes: map[string]*string{
					"ABC": aws.String("abc"),
					"DEF": aws.String("def"),
				},
			},
			wantErr: false,
		},
		{
			name: "error case",
			fields: fields{
				svc: &internal.SQSClientMock{
					GetQueueUrlWithContextFunc: func(ctx aws.Context, input *sqs.GetQueueUrlInput, opts ...request.Option) (output *sqs.GetQueueUrlOutput, e error) {
						return nil, errors.New("error get queue url with context")
					},
				},
			},
			args: args{
				ctx:  context.Background(),
				name: "foo",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "error case",
			fields: fields{
				svc: &internal.SQSClientMock{
					GetQueueUrlWithContextFunc: func(ctx aws.Context, input *sqs.GetQueueUrlInput, opts ...request.Option) (output *sqs.GetQueueUrlOutput, e error) {
						return &sqs.GetQueueUrlOutput{
							QueueUrl: aws.String("http://localhost/foo"),
						}, nil
					},
					GetQueueAttributesWithContextFunc: func(ctx aws.Context, input *sqs.GetQueueAttributesInput, opts ...request.Option) (output *sqs.GetQueueAttributesOutput, e error) {
						return nil, errors.New("error get queue attributes with context")
					},
				},
			},
			args: args{
				ctx:  context.Background(),
				name: "foo",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Connector{
				svc: tt.fields.svc,
			}
			got, err := c.resolveQueueAttributes(tt.args.ctx, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("Connector.resolveQueueAttributes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Connector.resolveQueueAttributes() = %v, want %v", got, tt.want)
			}

			if v, ok := c.name2queue.Load(tt.args.name); ok && !reflect.DeepEqual(v.(*internal.QueueAttributes), tt.want) {
				t.Errorf("Connector.resolveQueueAttributes() = %v, want %v", v, tt.want)
			}

			gotCash, err := c.resolveQueueAttributes(tt.args.ctx, tt.args.name)
			if !reflect.DeepEqual(gotCash, tt.want) {
				t.Errorf("Connector.resolveQueueAttributes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConnector_ChangeJobVisibility(t *testing.T) {

	svc := &internal.SQSClientMock{
		ChangeMessageVisibilityWithContextFunc: func(ctx aws.Context, input *sqs.ChangeMessageVisibilityInput, opts ...request.Option) (output *sqs.ChangeMessageVisibilityOutput, e error) {
			if aws.StringValue(input.ReceiptHandle) == "" {
				return nil, errors.New("ReceiptHandle is empty")
			}
			return &sqs.ChangeMessageVisibilityOutput{}, nil
		},
		GetQueueUrlWithContextFunc: func(ctx aws.Context, input *sqs.GetQueueUrlInput, opts ...request.Option) (output *sqs.GetQueueUrlOutput, e error) {
			if aws.StringValue(input.QueueName) == "" {
				return nil, errors.New("QueueName is empty")
			}
			return &sqs.GetQueueUrlOutput{
				QueueUrl: aws.String("http://localhost/foo"),
			}, nil
		},
		GetQueueAttributesWithContextFunc: func(ctx aws.Context, input *sqs.GetQueueAttributesInput, opts ...request.Option) (output *sqs.GetQueueAttributesOutput, e error) {
			if aws.StringValue(input.QueueUrl) == "" {
				return nil, errors.New("QueueUrl is empty")
			}
			return &sqs.GetQueueAttributesOutput{
				Attributes: map[string]*string{},
			}, nil
		},
	}

	type fields struct {
		name string
		svc  internal.SQSClient
	}
	type args struct {
		ctx   context.Context
		input *ChangeJobVisibilityInput
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *ChangeJobVisibilityOutput
		wantErr bool
	}{
		{
			name: "normal case",
			fields: fields{
				svc: svc,
			},
			args: args{
				ctx: context.Background(),
				input: &ChangeJobVisibilityInput{
					Job: &jobworker.Job{
						QueueName: "foo",
						Raw: &sqs.Message{
							ReceiptHandle: aws.String("receipt_handle"),
						},
					},
				},
			},
			want:    &ChangeJobVisibilityOutput{},
			wantErr: false,
		},
		{
			name: "error case",
			fields: fields{
				svc: svc,
			},
			args: args{
				ctx: context.Background(),
				input: &ChangeJobVisibilityInput{
					Job: &jobworker.Job{
						QueueName: "foo",
						Raw:       &sqs.Message{},
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "error case",
			fields: fields{
				svc: svc,
			},
			args: args{
				ctx: context.Background(),
				input: &ChangeJobVisibilityInput{
					Job: &jobworker.Job{
						Raw: &sqs.Message{
							ReceiptHandle: aws.String("receipt_handle"),
						},
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Connector{
				svc: tt.fields.svc,
			}
			got, err := c.ChangeJobVisibility(tt.args.ctx, tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Connector.ChangeJobVisibility() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Connector.ChangeJobVisibility() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConnector_FailJob(t *testing.T) {

	svc := &internal.SQSClientMock{
		ChangeMessageVisibilityWithContextFunc: func(ctx aws.Context, input *sqs.ChangeMessageVisibilityInput, opts ...request.Option) (output *sqs.ChangeMessageVisibilityOutput, e error) {
			if aws.StringValue(input.ReceiptHandle) == "" {
				return nil, errors.New("ReceiptHandle is empty")
			}
			return &sqs.ChangeMessageVisibilityOutput{}, nil
		},
		GetQueueUrlWithContextFunc: func(ctx aws.Context, input *sqs.GetQueueUrlInput, opts ...request.Option) (output *sqs.GetQueueUrlOutput, e error) {
			if aws.StringValue(input.QueueName) == "" {
				return nil, errors.New("QueueName is empty")
			}
			return &sqs.GetQueueUrlOutput{
				QueueUrl: aws.String("http://localhost/foo"),
			}, nil
		},
		GetQueueAttributesWithContextFunc: func(ctx aws.Context, input *sqs.GetQueueAttributesInput, opts ...request.Option) (output *sqs.GetQueueAttributesOutput, e error) {
			if aws.StringValue(input.QueueUrl) == "" {
				return nil, errors.New("QueueUrl is empty")
			}
			return &sqs.GetQueueAttributesOutput{
				Attributes: map[string]*string{},
			}, nil
		},
	}

	type fields struct {
		name string
		svc  internal.SQSClient
	}
	type args struct {
		ctx   context.Context
		input *jobworker.FailJobInput
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *jobworker.FailJobOutput
		wantErr bool
	}{
		{
			name: "normal case",
			fields: fields{
				name: "foo",
				svc:  svc,
			},
			args: args{
				ctx: context.Background(),
				input: &jobworker.FailJobInput{
					Job: &jobworker.Job{
						QueueName: "foo",
						Raw: &sqs.Message{
							ReceiptHandle: aws.String("receipt_handle"),
						},
					},
				},
			},
			want:    &jobworker.FailJobOutput{},
			wantErr: false,
		},
		{
			name: "error case",
			fields: fields{
				svc: svc,
			},
			args: args{
				ctx: context.Background(),
				input: &jobworker.FailJobInput{
					Job: &jobworker.Job{
						Raw: &sqs.Message{
							ReceiptHandle: aws.String("receipt_handle"),
						},
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "error case",
			fields: fields{
				svc: svc,
			},
			args: args{
				ctx: context.Background(),
				input: &jobworker.FailJobInput{
					Job: &jobworker.Job{
						QueueName: "foo",
						Raw:       &sqs.Message{},
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Connector{
				svc: tt.fields.svc,
			}
			got, err := c.FailJob(tt.args.ctx, tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Connector.FailJob() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Connector.FailJob() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConnector_CompleteJob(t *testing.T) {
	svc := &internal.SQSClientMock{
		DeleteMessageWithContextFunc: func(ctx aws.Context, input *sqs.DeleteMessageInput, opts ...request.Option) (output *sqs.DeleteMessageOutput, e error) {
			if aws.StringValue(input.ReceiptHandle) == "" {
				return nil, errors.New("ReceiptHandle is empty")
			}
			return &sqs.DeleteMessageOutput{}, nil
		},
		GetQueueUrlWithContextFunc: func(ctx aws.Context, input *sqs.GetQueueUrlInput, opts ...request.Option) (output *sqs.GetQueueUrlOutput, e error) {
			if aws.StringValue(input.QueueName) == "" {
				return nil, errors.New("QueueName is empty")
			}
			return &sqs.GetQueueUrlOutput{
				QueueUrl: aws.String("http://localhost/foo"),
			}, nil
		},
		GetQueueAttributesWithContextFunc: func(ctx aws.Context, input *sqs.GetQueueAttributesInput, opts ...request.Option) (output *sqs.GetQueueAttributesOutput, e error) {
			if aws.StringValue(input.QueueUrl) == "" {
				return nil, errors.New("QueueUrl is empty")
			}
			return &sqs.GetQueueAttributesOutput{
				Attributes: map[string]*string{},
			}, nil
		},
	}

	type fields struct {
		svc internal.SQSClient
	}
	type args struct {
		ctx   context.Context
		input *jobworker.CompleteJobInput
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *jobworker.CompleteJobOutput
		wantErr bool
	}{
		{
			name: "normal case",
			fields: fields{
				svc: svc,
			},
			args: args{
				ctx: context.Background(),
				input: &jobworker.CompleteJobInput{
					Job: &jobworker.Job{
						QueueName: "foo",
						Raw: &sqs.Message{
							ReceiptHandle: aws.String("receipt_handle"),
						},
					},
				},
			},
			want:    &jobworker.CompleteJobOutput{},
			wantErr: false,
		},
		{
			name: "error case",
			fields: fields{
				svc: svc,
			},
			args: args{
				ctx: context.Background(),
				input: &jobworker.CompleteJobInput{
					Job: &jobworker.Job{
						Raw: &sqs.Message{
							ReceiptHandle: aws.String("receipt_handle"),
						},
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "error case",
			fields: fields{
				svc: svc,
			},
			args: args{
				ctx: context.Background(),
				input: &jobworker.CompleteJobInput{
					Job: &jobworker.Job{
						QueueName: "foo",
						Raw:       &sqs.Message{},
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Connector{
				svc: tt.fields.svc,
			}
			got, err := c.CompleteJob(tt.args.ctx, tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Connector.CompleteJob() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Connector.CompleteJob() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConnector_Subscribe(t *testing.T) {

	svc := &internal.SQSClientMock{
		ReceiveMessageWithContextFunc: func(ctx aws.Context, input *sqs.ReceiveMessageInput, opts ...request.Option) (output *sqs.ReceiveMessageOutput, e error) {
			return &sqs.ReceiveMessageOutput{
				Messages: []*sqs.Message{
					{}, {}, {},
				},
			}, nil
		},
		GetQueueUrlWithContextFunc: func(ctx aws.Context, input *sqs.GetQueueUrlInput, opts ...request.Option) (output *sqs.GetQueueUrlOutput, e error) {
			if aws.StringValue(input.QueueName) == "" {
				return nil, errors.New("QueueName is empty")
			}
			return &sqs.GetQueueUrlOutput{
				QueueUrl: aws.String("http://localhost/foo"),
			}, nil
		},
		GetQueueAttributesWithContextFunc: func(ctx aws.Context, input *sqs.GetQueueAttributesInput, opts ...request.Option) (output *sqs.GetQueueAttributesOutput, e error) {
			if aws.StringValue(input.QueueUrl) == "" {
				return nil, errors.New("QueueUrl is empty")
			}
			return &sqs.GetQueueAttributesOutput{
				Attributes: map[string]*string{},
			}, nil
		},
	}

	type fields struct {
		svc internal.SQSClient
	}
	type args struct {
		ctx   context.Context
		input *jobworker.SubscribeInput
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *jobworker.SubscribeOutput
		wantErr bool
	}{
		{
			name: "normal case",
			fields: fields{
				svc: svc,
			},
			args: args{
				ctx: nil,
				input: &jobworker.SubscribeInput{
					Queue: "foo",
				},
			},
			wantErr: false,
		},
		{
			name: "error case",
			fields: fields{
				svc: svc,
			},
			args: args{
				ctx:   nil,
				input: &jobworker.SubscribeInput{},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Connector{
				svc: tt.fields.svc,
			}
			_, err := c.Subscribe(tt.args.ctx, tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Connector.Subscribe() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestConnector_Enqueue(t *testing.T) {

	svc := &internal.SQSClientMock{
		SendMessageWithContextFunc: func(ctx aws.Context, input *sqs.SendMessageInput, opts ...request.Option) (output *sqs.SendMessageOutput, e error) {
			if aws.StringValue(input.MessageBody) == "" {
				return nil, errors.New("MessageBody is empty")
			}
			return &sqs.SendMessageOutput{}, nil
		},
		GetQueueUrlWithContextFunc: func(ctx aws.Context, input *sqs.GetQueueUrlInput, opts ...request.Option) (output *sqs.GetQueueUrlOutput, e error) {
			if aws.StringValue(input.QueueName) == "" {
				return nil, errors.New("QueueName is empty")
			}
			return &sqs.GetQueueUrlOutput{
				QueueUrl: aws.String("http://localhost/foo"),
			}, nil
		},
		GetQueueAttributesWithContextFunc: func(ctx aws.Context, input *sqs.GetQueueAttributesInput, opts ...request.Option) (output *sqs.GetQueueAttributesOutput, e error) {
			if aws.StringValue(input.QueueUrl) == "" {
				return nil, errors.New("QueueUrl is empty")
			}
			return &sqs.GetQueueAttributesOutput{
				Attributes: map[string]*string{},
			}, nil
		},
	}

	type fields struct {
		svc internal.SQSClient
	}
	type args struct {
		ctx   context.Context
		input *jobworker.EnqueueInput
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *jobworker.EnqueueOutput
		wantErr bool
	}{
		{
			name: "normal case",
			fields: fields{
				svc: svc,
			},
			args: args{
				ctx: context.Background(),
				input: &jobworker.EnqueueInput{
					Queue:   "foo",
					Content: "hello",
				},
			},
			want:    &jobworker.EnqueueOutput{},
			wantErr: false,
		},
		{
			name: "error case",
			fields: fields{
				svc: svc,
			},
			args: args{
				ctx: context.Background(),
				input: &jobworker.EnqueueInput{
					Content: "hello",
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "error case",
			fields: fields{
				svc: svc,
			},
			args: args{
				ctx: context.Background(),
				input: &jobworker.EnqueueInput{
					Queue: "foo",
				},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Connector{
				svc: tt.fields.svc,
			}
			got, err := c.Enqueue(tt.args.ctx, tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Connector.Enqueue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Connector.Enqueue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_extractDelaySeconds(t *testing.T) {
	type args struct {
		meta map[string]string
	}
	tests := []struct {
		name    string
		args    args
		want    *int64
		wantNil bool
	}{
		{
			name: "normal case",
			args: args{
				meta: map[string]string{
					internal.MetadataKeyMessageDelaySeconds: "3",
				},
			},
			want:    aws.Int64(3),
			wantNil: false,
		},
		{
			name: "is nil",
			args: args{
				meta: map[string]string{},
			},
			want:    nil,
			wantNil: true,
		},
		{
			name: "is nil",
			args: args{
				meta: map[string]string{
					internal.MetadataKeyMessageDelaySeconds: "XXX",
				},
			},
			want:    nil,
			wantNil: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractDelaySeconds(tt.args.meta)
			if tt.wantNil {
				if got != nil {
					t.Errorf("extractDelaySeconds() = %v, want %v", got, "nil")
				}
				return
			}
			if *got != *tt.want {
				t.Errorf("extractDelaySeconds() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_extractGroupID(t *testing.T) {
	type args struct {
		meta map[string]string
	}
	tests := []struct {
		name string
		args args
		want *string
	}{
		{
			name: "normal case",
			args: args{
				meta: map[string]string{
					internal.MetadataKeyMessageGroupID: "blue",
				},
			},
			want: aws.String("blue"),
		},
		{
			name: "default case",
			args: args{
				meta: map[string]string{},
			},
			want: aws.String(defaultMessageGroupID),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractGroupID(tt.args.meta); *got != *tt.want {
				t.Errorf("extractGroupID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_extractDeduplicationID(t *testing.T) {
	type args struct {
		meta map[string]string
	}
	tests := []struct {
		name    string
		args    args
		want    *string
		wantNil bool
	}{
		{
			name: "normal case",
			args: args{
				meta: map[string]string{
					internal.MetadataKeyMessageDeduplicationID: "message-deduplication-id",
				},
			},
			want:    aws.String("message-deduplication-id"),
			wantNil: false,
		},
		{
			name: "is nil",
			args: args{
				meta: map[string]string{},
			},
			want:    nil,
			wantNil: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractDeduplicationID(tt.args.meta)
			if tt.wantNil {
				if got != nil {
					t.Errorf("extractDeduplicationID() = %v, want %v", got, "nil")
				}
				return
			}
			if *got != *tt.want {
				t.Errorf("extractDeduplicationID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_toSQSMessageAttributeValues(t *testing.T) {
	type args struct {
		attr map[string]*jobworker.CustomAttribute
	}
	tests := []struct {
		name string
		args args
		want map[string]*sqs.MessageAttributeValue
	}{
		{
			name: "normal case",
			args: args{
				attr: map[string]*jobworker.CustomAttribute{
					"Foo": {
						DataType:    "string",
						BinaryValue: nil,
						StringValue: "",
					},
				},
			},
			want: map[string]*sqs.MessageAttributeValue{
				"Foo": {
					BinaryValue: nil,
					DataType:    aws.String("string"),
					StringValue: aws.String(""),
				},
			},
		},
		{
			name: "is nil",
			args: args{
				attr: map[string]*jobworker.CustomAttribute{},
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := toSQSMessageAttributeValues(tt.args.attr); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("toSQSMessageAttributeValues() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_newSendMessageBatchRequestEntry(t *testing.T) {
	type args struct {
		id       string
		content  string
		metadata map[string]string
		attr     map[string]*jobworker.CustomAttribute
		queue    *internal.QueueAttributes
	}
	tests := []struct {
		name string
		args args
		want *sqs.SendMessageBatchRequestEntry
	}{
		{
			name: "normal case standard",
			args: args{
				id:      "uniq-id",
				content: "hello",
				metadata: map[string]string{
					internal.MetadataKeyMessageDelaySeconds:    "3",
					internal.MetadataKeyMessageDeduplicationID: "aaa",
					internal.MetadataKeyMessageGroupID:         "bbb",
				},
				queue: &internal.QueueAttributes{
					Name: "foo",
				},
			},
			want: &sqs.SendMessageBatchRequestEntry{
				DelaySeconds:      aws.Int64(3),
				Id:                aws.String("uniq-id"),
				MessageAttributes: nil,
				MessageBody:       aws.String("hello"),
			},
		},
		{
			name: "normal case fifo",
			args: args{
				id:      "uniq-id",
				content: "hello",
				metadata: map[string]string{
					internal.MetadataKeyMessageDelaySeconds:    "3",
					internal.MetadataKeyMessageDeduplicationID: "aaa",
					internal.MetadataKeyMessageGroupID:         "bbb",
				},
				queue: &internal.QueueAttributes{
					Name: "foo",
					RawAttributes: map[string]*string{
						internal.QueueAttributeKeyFifoQueue: aws.String("true"),
					},
				},
			},
			want: &sqs.SendMessageBatchRequestEntry{
				Id:                     aws.String("uniq-id"),
				MessageAttributes:      nil,
				MessageBody:            aws.String("hello"),
				MessageDeduplicationId: aws.String("aaa"),
				MessageGroupId:         aws.String("bbb"),
			},
		},
		{
			name: "normal case fifo and content based deduplication",
			args: args{
				id:      "uniq-id",
				content: "hello",
				metadata: map[string]string{
					internal.MetadataKeyMessageDelaySeconds: "3",
					internal.MetadataKeyMessageGroupID:      "bbb",
				},
				queue: &internal.QueueAttributes{
					Name: "foo",
					RawAttributes: map[string]*string{
						internal.QueueAttributeKeyFifoQueue:                 aws.String("true"),
						internal.QueueAttributeKeyContentBasedDeduplication: aws.String("true"),
					},
				},
			},
			want: &sqs.SendMessageBatchRequestEntry{
				Id:                     aws.String("uniq-id"),
				MessageAttributes:      nil,
				MessageBody:            aws.String("hello"),
				MessageDeduplicationId: aws.String("aaa"),
				MessageGroupId:         aws.String("bbb"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newSendMessageBatchRequestEntry(tt.args.id, tt.args.content, tt.args.metadata, tt.args.attr, tt.args.queue); !reflect.DeepEqual(got, tt.want) {
				if tt.args.queue.IsContentBasedDeduplication() {
					if !reflect.DeepEqual(got.MessageDeduplicationId, tt.want.MessageDeduplicationId) {
						return
					}
				}
				t.Errorf("newSendMessageBatchRequestEntry() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_newSendMessageInput(t *testing.T) {
	type args struct {
		content  string
		metadata map[string]string
		attr     map[string]*jobworker.CustomAttribute
		queue    *internal.QueueAttributes
	}
	tests := []struct {
		name string
		args args
		want *sqs.SendMessageInput
	}{
		{
			name: "normal case standard",
			args: args{
				content: "hello",
				metadata: map[string]string{
					internal.MetadataKeyMessageDelaySeconds:    "3",
					internal.MetadataKeyMessageDeduplicationID: "aaa",
					internal.MetadataKeyMessageGroupID:         "bbb",
				},
				queue: &internal.QueueAttributes{
					Name: "foo",
					URL:  "http://localhost/test",
				},
			},
			want: &sqs.SendMessageInput{
				QueueUrl:          aws.String("http://localhost/test"),
				DelaySeconds:      aws.Int64(3),
				MessageAttributes: nil,
				MessageBody:       aws.String("hello"),
			},
		},
		{
			name: "normal case fifo",
			args: args{
				content: "hello",
				metadata: map[string]string{
					internal.MetadataKeyMessageDelaySeconds:    "3",
					internal.MetadataKeyMessageDeduplicationID: "aaa",
					internal.MetadataKeyMessageGroupID:         "bbb",
				},
				queue: &internal.QueueAttributes{
					Name: "foo",
					URL:  "http://localhost/test",
					RawAttributes: map[string]*string{
						internal.QueueAttributeKeyFifoQueue: aws.String("true"),
					},
				},
			},
			want: &sqs.SendMessageInput{
				QueueUrl:               aws.String("http://localhost/test"),
				MessageAttributes:      nil,
				MessageBody:            aws.String("hello"),
				MessageDeduplicationId: aws.String("aaa"),
				MessageGroupId:         aws.String("bbb"),
			},
		},
		{
			name: "normal case fifo and content based deduplication",
			args: args{
				content: "hello",
				metadata: map[string]string{
					internal.MetadataKeyMessageDelaySeconds: "3",
					internal.MetadataKeyMessageGroupID:      "bbb",
				},
				queue: &internal.QueueAttributes{
					Name: "foo",
					URL:  "http://localhost/test",
					RawAttributes: map[string]*string{
						internal.QueueAttributeKeyFifoQueue:                 aws.String("true"),
						internal.QueueAttributeKeyContentBasedDeduplication: aws.String("true"),
					},
				},
			},
			want: &sqs.SendMessageInput{
				QueueUrl:               aws.String("http://localhost/test"),
				MessageAttributes:      nil,
				MessageBody:            aws.String("hello"),
				MessageDeduplicationId: aws.String("aaa"),
				MessageGroupId:         aws.String("bbb"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newSendMessageInput(tt.args.content, tt.args.metadata, tt.args.attr, tt.args.queue); !reflect.DeepEqual(got, tt.want) {
				if tt.args.queue.IsContentBasedDeduplication() {
					if !reflect.DeepEqual(got.MessageDeduplicationId, tt.want.MessageDeduplicationId) {
						return
					}
				}
				t.Errorf("newSendMessageInput() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConnector_EnqueueBatch(t *testing.T) {

	svc := &internal.SQSClientMock{
		SendMessageBatchWithContextFunc: func(ctx aws.Context, input *sqs.SendMessageBatchInput, opts ...request.Option) (output *sqs.SendMessageBatchOutput, e error) {
			if len(input.Entries) == 0 {
				return nil, errors.New("entries is empty")
			}
			return &sqs.SendMessageBatchOutput{
				Failed: nil,
				Successful: []*sqs.SendMessageBatchResultEntry{
					{
						Id: aws.String("a-1"),
					},
					{
						Id: aws.String("a-2"),
					},
					{
						Id: aws.String("a-3"),
					},
				},
			}, nil
		},
		GetQueueUrlWithContextFunc: func(ctx aws.Context, input *sqs.GetQueueUrlInput, opts ...request.Option) (output *sqs.GetQueueUrlOutput, e error) {
			if aws.StringValue(input.QueueName) == "" {
				return nil, errors.New("QueueName is empty")
			}
			return &sqs.GetQueueUrlOutput{
				QueueUrl: aws.String("http://localhost/foo"),
			}, nil
		},
		GetQueueAttributesWithContextFunc: func(ctx aws.Context, input *sqs.GetQueueAttributesInput, opts ...request.Option) (output *sqs.GetQueueAttributesOutput, e error) {
			if aws.StringValue(input.QueueUrl) == "" {
				return nil, errors.New("QueueUrl is empty")
			}
			return &sqs.GetQueueAttributesOutput{
				Attributes: map[string]*string{},
			}, nil
		},
	}

	type fields struct {
		svc internal.SQSClient
	}
	type args struct {
		ctx   context.Context
		input *jobworker.EnqueueBatchInput
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *jobworker.EnqueueBatchOutput
		wantErr bool
	}{
		{
			name: "normal case",
			fields: fields{
				svc: svc,
			},
			args: args{
				ctx: context.Background(),
				input: &jobworker.EnqueueBatchInput{
					Queue: "foo",
					Entries: []*jobworker.EnqueueBatchEntry{
						{
							ID:      "a-1",
							Content: "foo",
						},
						{
							ID:      "a-2",
							Content: "bar",
						},
						{
							ID:      "a-2",
							Content: "baz",
						},
					},
				},
			},
			want: &jobworker.EnqueueBatchOutput{
				Failed:     nil,
				Successful: []string{"a-1", "a-2", "a-3"},
			},
			wantErr: false,
		},
		{
			name: "error case",
			fields: fields{
				svc: svc,
			},
			args: args{
				ctx: context.Background(),
				input: &jobworker.EnqueueBatchInput{
					Entries: []*jobworker.EnqueueBatchEntry{
						{
							ID:      "a-1",
							Content: "foo",
						},
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "error case",
			fields: fields{
				svc: svc,
			},
			args: args{
				ctx: context.Background(),
				input: &jobworker.EnqueueBatchInput{
					Queue:   "foo",
					Entries: []*jobworker.EnqueueBatchEntry{},
				},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Connector{
				svc: tt.fields.svc,
			}
			got, err := c.EnqueueBatch(tt.args.ctx, tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Connector.EnqueueBatch() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Connector.EnqueueBatch() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestProvider_Open(t *testing.T) {
	type args struct {
		attrs map[string]interface{}
	}
	tests := []struct {
		name    string
		p       Provider
		args    args
		want    jobworker.Connector
		wantErr bool
	}{
		{
			name: "normal case",
			p:    Provider{},
			args: args{
				attrs: map[string]interface{}{
					"Region":          "us-east-1",
					"AccessKeyID":     "foo",
					"SecretAccessKey": "bar",
					"NumMaxRetries":   3,
				},
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := Provider{}
			_, err := p.Open(tt.args.attrs)
			if (err != nil) != tt.wantErr {
				t.Errorf("Provider.Open() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_extractReceiptHandle(t *testing.T) {
	type args struct {
		job *jobworker.Job
	}
	tests := []struct {
		name string
		args args
		want *string
	}{
		{
			name: "normal case",
			args: args{
				job: &jobworker.Job{
					Raw: &sqs.Message{
						ReceiptHandle: aws.String("foo"),
					},
				},
			},
			want: aws.String("foo"),
		},
		{
			name: "is nil",
			args: args{
				job: &jobworker.Job{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractReceiptHandle(tt.args.job); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("extractReceiptHandle() = %v, want %v", got, tt.want)
			}
		})
	}
}
