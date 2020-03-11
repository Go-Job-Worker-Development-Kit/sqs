package sqs

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/go-jwdk/jobworker"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/go-jwdk/aws-sqs-connector/internal"
)

func TestConnector_resolveQueueAttributes(t *testing.T) {
	type fields struct {
		name string
		svc  internal.SQSClient
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
				name: "",
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
				name: "",
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
				name: "",
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
				name: tt.fields.name,
				svc:  tt.fields.svc,
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
				name: tt.fields.name,
				svc:  tt.fields.svc,
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
