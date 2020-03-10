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

type SQSClientMock struct {
	SendMessageWithContextFunc             func(ctx aws.Context, input *sqs.SendMessageInput, opts ...request.Option) (*sqs.SendMessageOutput, error)
	SendMessageBatchWithContextFunc        func(ctx aws.Context, input *sqs.SendMessageBatchInput, opts ...request.Option) (*sqs.SendMessageBatchOutput, error)
	ReceiveMessageWithContextFunc          func(ctx aws.Context, input *sqs.ReceiveMessageInput, opts ...request.Option) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageWithContextFunc           func(ctx aws.Context, input *sqs.DeleteMessageInput, opts ...request.Option) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibilityWithContextFunc func(ctx aws.Context, input *sqs.ChangeMessageVisibilityInput, opts ...request.Option) (*sqs.ChangeMessageVisibilityOutput, error)
	GetQueueUrlWithContextFunc             func(ctx aws.Context, input *sqs.GetQueueUrlInput, opts ...request.Option) (*sqs.GetQueueUrlOutput, error)
	GetQueueAttributesWithContextFunc      func(ctx aws.Context, input *sqs.GetQueueAttributesInput, opts ...request.Option) (*sqs.GetQueueAttributesOutput, error)
	CreateQueueWithContextFunc             func(ctx aws.Context, input *sqs.CreateQueueInput, opts ...request.Option) (*sqs.CreateQueueOutput, error)
	SetQueueAttributesWithContextFunc      func(ctx aws.Context, input *sqs.SetQueueAttributesInput, opts ...request.Option) (*sqs.SetQueueAttributesOutput, error)
}

func (m *SQSClientMock) SendMessageWithContext(ctx aws.Context, input *sqs.SendMessageInput, opts ...request.Option) (*sqs.SendMessageOutput, error) {
	if m.SendMessageWithContextFunc == nil {
		panic("This method is not defined.")
	}
	return m.SendMessageWithContextFunc(ctx, input, opts...)
}

func (m *SQSClientMock) SendMessageBatchWithContext(ctx aws.Context, input *sqs.SendMessageBatchInput, opts ...request.Option) (*sqs.SendMessageBatchOutput, error) {
	if m.SendMessageBatchWithContextFunc == nil {
		panic("This method is not defined.")
	}
	return m.SendMessageBatchWithContextFunc(ctx, input, opts...)
}

func (m *SQSClientMock) ReceiveMessageWithContext(ctx aws.Context, input *sqs.ReceiveMessageInput, opts ...request.Option) (*sqs.ReceiveMessageOutput, error) {
	if m.ReceiveMessageWithContextFunc == nil {
		panic("This method is not defined.")
	}
	return m.ReceiveMessageWithContextFunc(ctx, input, opts...)
}

func (m *SQSClientMock) DeleteMessageWithContext(ctx aws.Context, input *sqs.DeleteMessageInput, opts ...request.Option) (*sqs.DeleteMessageOutput, error) {
	if m.DeleteMessageWithContextFunc == nil {
		panic("This method is not defined.")
	}
	return m.DeleteMessageWithContextFunc(ctx, input, opts...)
}

func (m *SQSClientMock) ChangeMessageVisibilityWithContext(ctx aws.Context, input *sqs.ChangeMessageVisibilityInput, opts ...request.Option) (*sqs.ChangeMessageVisibilityOutput, error) {
	if m.ChangeMessageVisibilityWithContextFunc == nil {
		panic("This method is not defined.")
	}
	return m.ChangeMessageVisibilityWithContextFunc(ctx, input, opts...)
}

func (m *SQSClientMock) GetQueueUrlWithContext(ctx aws.Context, input *sqs.GetQueueUrlInput, opts ...request.Option) (*sqs.GetQueueUrlOutput, error) {
	if m.GetQueueUrlWithContextFunc == nil {
		panic("This method is not defined.")
	}
	return m.GetQueueUrlWithContextFunc(ctx, input, opts...)
}

func (m *SQSClientMock) GetQueueAttributesWithContext(ctx aws.Context, input *sqs.GetQueueAttributesInput, opts ...request.Option) (*sqs.GetQueueAttributesOutput, error) {
	if m.GetQueueAttributesWithContextFunc == nil {
		panic("This method is not defined.")
	}
	return m.GetQueueAttributesWithContextFunc(ctx, input, opts...)
}

func (m *SQSClientMock) CreateQueueWithContext(ctx aws.Context, input *sqs.CreateQueueInput, opts ...request.Option) (*sqs.CreateQueueOutput, error) {
	if m.CreateQueueWithContextFunc == nil {
		panic("This method is not defined.")
	}
	return m.CreateQueueWithContextFunc(ctx, input, opts...)
}

func (m *SQSClientMock) SetQueueAttributesWithContext(ctx aws.Context, input *sqs.SetQueueAttributesInput, opts ...request.Option) (*sqs.SetQueueAttributesOutput, error) {
	if m.SetQueueAttributesWithContextFunc == nil {
		panic("This method is not defined.")
	}
	return m.SetQueueAttributesWithContextFunc(ctx, input, opts...)
}
