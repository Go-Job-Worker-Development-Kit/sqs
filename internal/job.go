package internal

import (
	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/go-job-worker-development-kit/jobworker"
)

func newJob(queue string, msg *sqs.Message, conn jobworker.Connector) *jobworker.Job {
	var payload jobworker.Payload
	payload.Content = aws.StringValue(msg.Body)
	payload.Metadata = newMetadata(msg)
	payload.CustomAttribute = newCustomAttribute(msg)
	payload.Raw = msg
	return jobworker.NewJob(conn, queue, &payload)
}

// Metadata value examples below:
//
// SecID                  uint64
// ReceiptID              string
// DeduplicationID        string
// GroupID                string
// InvisibleUntil         int64
// RetryCount             int64
// EnqueueAt              int64
// MessageId              string
// ReceiptHandle          string
// MD5OfBody              string
// MD5OfMessageAttributes string
func newMetadata(msg *sqs.Message) map[string]string {
	metadata := make(map[string]string)
	metadata = make(map[string]string)
	for k, v := range msg.Attributes {
		if v != nil {
			metadata[k] = aws.StringValue(v)
		}
	}
	metadata["MessageId"] = aws.StringValue(msg.MessageId)
	metadata["ReceiptHandle"] = aws.StringValue(msg.ReceiptHandle)
	metadata["MD5OfBody"] = aws.StringValue(msg.MD5OfBody)
	metadata["MD5OfMessageAttributes"] = aws.StringValue(msg.MD5OfMessageAttributes)
	return metadata
}

func newCustomAttribute(msg *sqs.Message) map[string]*jobworker.CustomAttribute {
	customAttribute := make(map[string]*jobworker.CustomAttribute)
	for k, v := range msg.MessageAttributes {
		if v != nil {
			customAttribute[k] = &jobworker.CustomAttribute{
				DataType:    aws.StringValue(v.DataType),
				BinaryValue: v.BinaryValue,
				StringValue: aws.StringValue(v.StringValue),
			}
		}
	}
	return customAttribute
}
