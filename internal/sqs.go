package internal

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type SQSClient interface {
	SendMessageWithContext(ctx aws.Context, input *sqs.SendMessageInput, opts ...request.Option) (*sqs.SendMessageOutput, error)
	SendMessageBatchWithContext(ctx aws.Context, input *sqs.SendMessageBatchInput, opts ...request.Option) (*sqs.SendMessageBatchOutput, error)
	ReceiveMessageWithContext(ctx aws.Context, input *sqs.ReceiveMessageInput, opts ...request.Option) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageWithContext(ctx aws.Context, input *sqs.DeleteMessageInput, opts ...request.Option) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibilityWithContext(ctx aws.Context, input *sqs.ChangeMessageVisibilityInput, opts ...request.Option) (*sqs.ChangeMessageVisibilityOutput, error)
	GetQueueUrlWithContext(ctx aws.Context, input *sqs.GetQueueUrlInput, opts ...request.Option) (*sqs.GetQueueUrlOutput, error)
	GetQueueAttributesWithContext(ctx aws.Context, input *sqs.GetQueueAttributesInput, opts ...request.Option) (*sqs.GetQueueAttributesOutput, error)
	CreateQueueWithContext(ctx aws.Context, input *sqs.CreateQueueInput, opts ...request.Option) (*sqs.CreateQueueOutput, error)
	SetQueueAttributesWithContext(ctx aws.Context, input *sqs.SetQueueAttributesInput, opts ...request.Option) (*sqs.SetQueueAttributesOutput, error)
}
