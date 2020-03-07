package internal

import (
	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/go-jwdk/jobworker"
)

func newJob(queueName string, msg *sqs.Message, conn jobworker.Connector) *jobworker.Job {
	return &jobworker.Job{
		Conn:            conn,
		QueueName:       queueName,
		Content:         aws.StringValue(msg.Body),
		Metadata:        newMetadata(msg),
		CustomAttribute: newCustomAttribute(msg),
		Raw:             msg,
	}
}

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
