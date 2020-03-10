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
