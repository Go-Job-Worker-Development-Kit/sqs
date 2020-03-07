package sqs

import (
	"context"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/go-jwdk/aws-sqs-connector/internal"
	"github.com/go-jwdk/jobworker"
	uuid "github.com/satori/go.uuid"
)

const (
	connName  = "sqs"
	pkgName   = "aws-sqs-connector"
	logPrefix = "[" + pkgName + "]"

	connAttributeNameAwsRegion          = "Region"
	connAttributeNameAwsAccessKeyID     = "AccessKeyID"
	connAttributeNameAwsSecretAccessKey = "SecretAccessKey"
	connAttributeNameAwsSessionToken    = "SessionToken"
	connAttributeNameNumMaxRetries      = "NumMaxRetries"

	defaultNumMaxRetries = 3
)

func init() {
	jobworker.Register(connName, &Provider{})
}

type Provider struct {
}

func (Provider) Open(attrs map[string]interface{}) (jobworker.Connector, error) {
	return Open(attrs)
}

func Open(attrs map[string]interface{}) (*Connector, error) {
	values := connAttrsToValues(attrs)
	values.applyDefaultValues()

	var awsCfg aws.Config
	awsCfg.Region = aws.String(values.region)
	if (values.accessKeyID != "" && values.secretAccessKey != "") || values.sessionToken != "" {
		awsCfg.Credentials = credentials.NewStaticCredentials(
			values.accessKeyID,
			values.secretAccessKey,
			values.sessionToken)
	}
	awsCfg.MaxRetries = values.numMaxRetries

	sess, err := session.NewSession(&awsCfg)
	if err != nil {
		return nil, err
	}
	// Create a SQS service client.
	svc := sqs.New(sess)

	return OpenWithSQS(svc)
}

func OpenWithSQS(svc *sqs.SQS) (*Connector, error) {
	return &Connector{
		name: connName,
		svc:  svc,
	}, nil
}

type values struct {
	region          string
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
	numMaxRetries   *int
}

func (v *values) applyDefaultValues() {
	if v.numMaxRetries == nil {
		i := defaultNumMaxRetries
		v.numMaxRetries = &i
	}
}

func connAttrsToValues(attrs map[string]interface{}) *values {
	var values values
	for k, v := range attrs {
		switch k {
		case connAttributeNameAwsRegion:
			s := v.(string)
			values.region = s
		case connAttributeNameAwsAccessKeyID:
			s := v.(string)
			values.accessKeyID = s
		case connAttributeNameAwsSecretAccessKey:
			s := v.(string)
			values.secretAccessKey = s
		case connAttributeNameAwsSessionToken:
			s := v.(string)
			values.sessionToken = s
		case connAttributeNameNumMaxRetries:
			i := v.(int)
			values.numMaxRetries = &i
		}
	}
	return &values
}

type Connector struct {
	name       string
	svc        *sqs.SQS
	name2queue sync.Map
	loggerFunc jobworker.LoggerFunc
}

func (c *Connector) Name() string {
	return c.name
}

func (c *Connector) Subscribe(ctx context.Context, input *jobworker.SubscribeInput) (*jobworker.SubscribeOutput, error) {
	queue, err := c.resolveQueue(ctx, input.Queue)
	if err != nil {
		return nil, err
	}
	sub := internal.NewSubscription(queue.URL, c.svc, c)
	go sub.ReadLoop()
	return &jobworker.SubscribeOutput{
		Subscription: sub,
	}, nil

}

func (c *Connector) Enqueue(ctx context.Context, input *jobworker.EnqueueInput) (*jobworker.EnqueueOutput, error) {
	queue, err := c.resolveQueue(ctx, input.Queue)
	if err != nil {
		// TODO
		return nil, err
	}
	_, err = c.svc.SendMessageWithContext(ctx, newSendMessageInput(input.Payload, input.Metadata, input.CustomAttribute, queue))
	if err != nil {
		return nil, err
	}
	return &jobworker.EnqueueOutput{}, nil
}

func (c *Connector) EnqueueBatch(ctx context.Context, input *jobworker.EnqueueBatchInput) (*jobworker.EnqueueBatchOutput, error) {

	queue, err := c.resolveQueue(ctx, input.Queue)
	if err != nil {
		// TODO
		return nil, err
	}

	var entries []*sqs.SendMessageBatchRequestEntry
	for k, v := range input.Id2Content {
		entries = append(entries, newSendMessageBatchRequestEntry(k, v, input.Metadata, input.CustomAttribute, queue))
	}

	sqsOutput, err := c.svc.SendMessageBatchWithContext(ctx, &sqs.SendMessageBatchInput{
		Entries:  entries,
		QueueUrl: &queue.URL,
	})

	var output jobworker.EnqueueBatchOutput
	if sqsOutput != nil {
		for _, v := range sqsOutput.Successful {
			output.Successful = append(output.Successful, aws.StringValue(v.Id))
		}
		for _, v := range sqsOutput.Failed {
			output.Failed = append(output.Failed, aws.StringValue(v.Id))
		}
	}
	return &output, err
}

func newSendMessageInput(payload string, metadata map[string]string, attr map[string]*jobworker.CustomAttribute, queue *internal.Queue) *sqs.SendMessageInput {
	var input sqs.SendMessageInput
	input.MessageBody = aws.String(payload)
	input.QueueUrl = aws.String(queue.URL)

	input.MessageAttributes = make(map[string]*sqs.MessageAttributeValue)
	for k, v := range attr {
		input.MessageAttributes[k] = &sqs.MessageAttributeValue{
			DataType:    aws.String(v.DataType),
			StringValue: aws.String(v.StringValue),
			BinaryValue: v.BinaryValue,
		}
	}

	if v, ok := queue.Attributes[internal.QueueAttributeKeyFifoQueue]; ok && aws.StringValue(v) == "true" {

		// fifo only
		messageGroupId := metadata[internal.MetadataKeyMessageGroupID]
		if messageGroupId == "" {
			messageGroupId = "default"
		}
		input.MessageGroupId = aws.String(messageGroupId)

		if v := metadata[internal.MetadataKeyMessageDeduplicationID]; v != "" {
			input.MessageDeduplicationId = aws.String(v)
		}

		if v, ok := queue.Attributes[internal.QueueAttributeKeyContentBasedDeduplication]; ok && aws.StringValue(v) != "true" {
			if input.MessageDeduplicationId == nil {
				id := uuid.NewV4().String()
				input.MessageDeduplicationId = aws.String(id)
			}
		}

	} else {
		// standard only
		v, ok := metadata[internal.MetadataKeyMessageDelaySeconds]
		if ok {
			delaySeconds, _ := strconv.ParseInt(v, 10, 64)
			input.DelaySeconds = aws.Int64(delaySeconds)
		}
	}

	return &input
}

func newSendMessageBatchRequestEntry(id string, payload string,
	metadata map[string]string, attr map[string]*jobworker.CustomAttribute, queue *internal.Queue) *sqs.SendMessageBatchRequestEntry {
	var entry sqs.SendMessageBatchRequestEntry
	entry.Id = aws.String(id)
	entry.MessageBody = aws.String(payload)
	entry.MessageAttributes = make(map[string]*sqs.MessageAttributeValue)
	for k, v := range attr {
		entry.MessageAttributes[k] = &sqs.MessageAttributeValue{
			DataType:    aws.String(v.DataType),
			StringValue: aws.String(v.StringValue),
			BinaryValue: v.BinaryValue,
		}
	}

	if v, ok := queue.Attributes[internal.QueueAttributeKeyFifoQueue]; ok && aws.StringValue(v) == "true" {

		// fifo only
		messageGroupId := metadata[internal.MetadataKeyMessageGroupID]
		if messageGroupId == "" {
			messageGroupId = "default"
		}
		entry.MessageGroupId = aws.String(messageGroupId)

		if v := metadata[internal.MetadataKeyMessageDeduplicationID]; v != "" {
			entry.MessageDeduplicationId = aws.String(v)
		}

		if v, ok := queue.Attributes[internal.QueueAttributeKeyContentBasedDeduplication]; ok && aws.StringValue(v) != "true" {
			if entry.MessageDeduplicationId == nil {
				id := uuid.NewV4().String()
				entry.MessageDeduplicationId = aws.String(id)
			}
		}

	} else {
		// standard only
		v, ok := metadata[internal.MetadataKeyMessageDelaySeconds]
		if ok {
			delaySeconds, _ := strconv.ParseInt(v, 10, 64)
			entry.DelaySeconds = aws.Int64(delaySeconds)
		}
	}

	return &entry
}

func (c *Connector) CompleteJob(ctx context.Context, input *jobworker.CompleteJobInput) (*jobworker.CompleteJobOutput, error) {
	queue, err := c.resolveQueue(ctx, input.Job.QueueName)
	if err != nil {
		// TODO
		return nil, err
	}
	_, err = c.svc.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &queue.URL,
		ReceiptHandle: aws.String(input.Job.Metadata[internal.MetadataKeyReceiptHandle]),
	})
	if err != nil {
		// TODO handle already delete error
		return nil, err
	}
	return &jobworker.CompleteJobOutput{}, nil
}

func (c *Connector) FailJob(ctx context.Context, input *jobworker.FailJobInput) (*jobworker.FailJobOutput, error) {
	_, err := c.ChangeJobVisibility(ctx, &ChangeJobVisibilityInput{
		Job:               input.Job,
		VisibilityTimeout: 0,
	})
	return &jobworker.FailJobOutput{}, err
}

func (c *Connector) Close() error {
	c.debug("no 'Close' available")
	return nil
}

func (c *Connector) SetLoggerFunc(f jobworker.LoggerFunc) {
	c.loggerFunc = f
}

func (c *Connector) resolveQueue(ctx context.Context, name string) (*internal.Queue, error) {

	v, ok := c.name2queue.Load(name)
	if !ok || v == nil {

		urlOutput, err := c.svc.GetQueueUrlWithContext(ctx, &sqs.GetQueueUrlInput{
			QueueName: aws.String(name),
		})
		if err != nil {

			return nil, err
		}

		attrsOutput, err := c.svc.GetQueueAttributesWithContext(ctx, &sqs.GetQueueAttributesInput{
			AttributeNames: []*string{
				aws.String(sqs.QueueAttributeNameAll),
			},
			QueueUrl: urlOutput.QueueUrl,
		})
		if err != nil {

			return nil, err
		}

		v = &internal.Queue{
			URL:        aws.StringValue(urlOutput.QueueUrl),
			Attributes: attrsOutput.Attributes,
		}

		c.name2queue.Store(name, v)
	}

	return v.(*internal.Queue), nil
}

func (c *Connector) debug(args ...interface{}) {
	if c.verbose() {
		args = append([]interface{}{logPrefix}, args...)
		c.loggerFunc(args...)
	}
}

func (c *Connector) verbose() bool {
	return c.loggerFunc != nil
}

func (c *Connector) ChangeJobVisibility(ctx context.Context, input *ChangeJobVisibilityInput) (*ChangeJobVisibilityOutput, error) {
	queue, err := c.resolveQueue(ctx, input.Job.QueueName)
	if err != nil {
		// TODO
		return nil, err
	}
	_, err = c.svc.ChangeMessageVisibilityWithContext(ctx, &sqs.ChangeMessageVisibilityInput{
		ReceiptHandle:     aws.String(input.Job.Metadata[internal.MetadataKeyReceiptHandle]),
		QueueUrl:          aws.String(queue.URL),
		VisibilityTimeout: aws.Int64(input.VisibilityTimeout),
	})
	return &ChangeJobVisibilityOutput{}, nil
}

func (c *Connector) CreateQueue(ctx context.Context, input *CreateQueueInput) (*CreateQueueOutput, error) {

	attributes := make(map[string]*string)
	for k, v := range input.Attributes {
		s := v.(string)
		attributes[k] = &s
	}

	_, err := c.svc.CreateQueueWithContext(ctx, &sqs.CreateQueueInput{
		QueueName:  aws.String(input.Name),
		Attributes: attributes,
	})
	if err != nil {
		// TODO
		return nil, err
	}
	return &CreateQueueOutput{}, nil
}

func (c *Connector) UpdateQueue(ctx context.Context, input *UpdateQueueInput) (*UpdateQueueOutput, error) {
	queue, err := c.resolveQueue(ctx, input.Name)
	if err != nil {
		// TODO
		return nil, err
	}

	attributes := make(map[string]*string)
	for k, v := range input.Attributes {
		s := v.(string)
		attributes[k] = &s
	}

	_, err = c.svc.SetQueueAttributesWithContext(ctx, &sqs.SetQueueAttributesInput{
		QueueUrl:   aws.String(queue.URL),
		Attributes: attributes,
	})
	if err != nil {
		// TODO
		return nil, err
	}
	return &UpdateQueueOutput{}, nil
}

// TODO
//func (c *Connector) RedriveJob(ctx context.Context, input *RedriveJobInput, opts ...func(*jobworker.Option)) (*RedriveJobOutput, error) {
//	fromQueue, err := c.resolveQueue(ctx, input.From)
//	if err != nil {
//		// TODO
//		return nil, err
//	}
//
//	toQueue, err := c.resolveQueue(ctx, input.To)
//	if err != nil {
//		// TODO
//		return nil, err
//	}
//
//	waitTimeSeconds := int64(2)
//	maxNumberOfMessages := int64(10)
//	visibilityTimeout := int64(20)
//
//	var reviveMsg *sqs.Message
//
//L:
//	for {
//
//		select {
//		case <-ctx.Done():
//			// TODO
//			return nil, errors.New("timeout")
//		default:
//			result, err := c.svc.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
//				AttributeNames: []*string{
//					aws.String(sqs.QueueAttributeNameAll),
//				},
//				MessageAttributeNames: []*string{
//					aws.String(sqs.QueueAttributeNameAll),
//				},
//				QueueUrl:            aws.String(fromQueue.URL),
//				MaxNumberOfMessages: aws.Int64(maxNumberOfMessages),
//				VisibilityTimeout:   aws.Int64(visibilityTimeout),
//				WaitTimeSeconds:     aws.Int64(waitTimeSeconds),
//			})
//			if err != nil {
//				// TODO
//				return nil, err
//			}
//			for _, msg := range result.Messages {
//				if aws.StringValue(msg.MessageId) == input.Target {
//					reviveMsg = msg
//					break L
//				}
//			}
//		}
//	}
//
//	messageDeduplicationId := reviveMsg.Attributes[sqs.MessageSystemAttributeNameMessageDeduplicationId]
//	messageGroupId := reviveMsg.Attributes[sqs.MessageSystemAttributeNameMessageGroupId]
//	class := reviveMsg.MessageAttributes[messageAttributeNameJobClass]
//
//	_, err = c.svc.SendMessageWithContext(ctx, &sqs.SendMessageInput{
//		DelaySeconds: aws.Int64(input.DelaySeconds),
//		MessageAttributes: map[string]*sqs.MessageAttributeValue{
//			messageAttributeNameJobClass: {
//				DataType:    aws.String("String"),
//				StringValue: class.StringValue,
//			},
//		},
//		MessageBody:             reviveMsg.Body,
//		MessageDeduplicationId:  messageDeduplicationId,
//		MessageGroupId:          messageGroupId,
//		MessageSystemAttributes: map[string]*sqs.MessageSystemAttributeValue{},
//		QueueUrl:                aws.String(toQueue.URL),
//	})
//	if err != nil {
//		// TODO
//		return nil, err
//	}
//
//	_, err = c.svc.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
//		QueueUrl:      aws.String(fromQueue.URL),
//		ReceiptHandle: reviveMsg.ReceiptHandle,
//	})
//	if err != nil {
//		// TODO
//		return nil, err
//	}
//	return &RedriveJobOutput{}, nil
//
//}

type ChangeJobVisibilityInput struct {
	Job               *jobworker.Job
	VisibilityTimeout int64
}

type ChangeJobVisibilityOutput struct{}

type CreateQueueInput struct {
	Name       string
	Attributes map[string]interface{}
}

type CreateQueueOutput struct{}

type UpdateQueueInput struct {
	Name       string
	Attributes map[string]interface{}
}

type UpdateQueueOutput struct{}

type RedriveJobInput struct {
	From         string
	To           string
	Target       string
	DelaySeconds int64
}

type RedriveJobOutput struct{}
