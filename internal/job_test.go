package internal

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/go-jwdk/jobworker"
)

func Test_newJob(t *testing.T) {
	type args struct {
		queueName string
		msg       *sqs.Message
		conn      jobworker.Connector
	}
	tests := []struct {
		name string
		args args
		want *jobworker.Job
	}{
		{
			name: "normal case",
			args: args{
				queueName: "test-q",
				msg: &sqs.Message{
					Body: aws.String("hello"),
				},
			},
			want: &jobworker.Job{
				QueueName: "test-q",
				Content:   "hello",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newJob(tt.args.queueName, tt.args.msg, tt.args.conn)
			if !reflect.DeepEqual(got.QueueName, tt.want.QueueName) {
				t.Errorf("newJob() = %v, want %v", got.QueueName, tt.want.QueueName)
			}
			if !reflect.DeepEqual(got.Content, tt.want.Content) {
				t.Errorf("newJob() = %v, want %v", got.Content, tt.want.Content)
			}
		})
	}
}

func Test_newMetadata(t *testing.T) {
	type args struct {
		msg *sqs.Message
	}
	tests := []struct {
		name string
		args args
		want map[string]string
	}{
		{
			name: "normal case",
			args: args{
				msg: &sqs.Message{
					Attributes: map[string]*string{
						"ApproximateReceiveCount": aws.String("3"),
						"MessageDeduplicationId":  aws.String("message_deduplication_id"),
						"MessageGroupId":          aws.String("message_group_id"),
					},
					MD5OfBody:              aws.String("md5_of_body"),
					MD5OfMessageAttributes: aws.String("md5_of_message_attributes"),
					MessageId:              aws.String("message_id"),
					ReceiptHandle:          aws.String("receipt_handle"),
				},
			},
			want: map[string]string{
				"ApproximateReceiveCount":         "3",
				"MessageDeduplicationId":          "message_deduplication_id",
				"MessageGroupId":                  "message_group_id",
				MetadataKeyMD5OfBody:              "md5_of_body",
				MetadataKeyMD5OfMessageAttributes: "md5_of_message_attributes",
				MetadataKeyMessageId:              "message_id",
				MetadataKeyReceiptHandle:          "receipt_handle",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newMetadata(tt.args.msg); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newMetadata() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_newCustomAttribute(t *testing.T) {
	type args struct {
		msg *sqs.Message
	}
	tests := []struct {
		name string
		args args
		want map[string]*jobworker.CustomAttribute
	}{
		{
			name: "normal case",
			args: args{
				msg: &sqs.Message{
					MessageAttributes: map[string]*sqs.MessageAttributeValue{
						"Foo": {
							DataType:    aws.String("String"),
							BinaryValue: nil,
							StringValue: aws.String("hello"),
						},
						"Bar": {
							DataType:    aws.String("Number"),
							BinaryValue: nil,
							StringValue: aws.String("123"),
						},
						"Baz": {
							DataType:    aws.String("Binary"),
							BinaryValue: []byte("hello"),
							StringValue: nil,
						},
					},
				},
			},
			want: map[string]*jobworker.CustomAttribute{
				"Foo": {
					DataType:    "String",
					BinaryValue: nil,
					StringValue: "hello",
				},
				"Bar": {
					DataType:    "Number",
					BinaryValue: nil,
					StringValue: "123",
				},
				"Baz": {
					DataType:    "Binary",
					BinaryValue: []byte("hello"),
					StringValue: "",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newCustomAttribute(tt.args.msg); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newCustomAttribute() = %v, want %v", got, tt.want)
			}
		})
	}
}
