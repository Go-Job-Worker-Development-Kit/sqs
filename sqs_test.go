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
