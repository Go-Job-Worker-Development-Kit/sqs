package sqs

import (
	"context"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go/aws/endpoints"

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
	connAttributeNameEndpointURL        = "EndpointURL"

	defaultNumMaxRetries  = 3
	defaultMessageGroupID = "default"
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
	awsCfg.CredentialsChainVerboseErrors = aws.Bool(true)
	awsCfg.Region = aws.String(values.region)
	if values.accessKeyID != "" || values.secretAccessKey != "" || values.sessionToken != "" {
		awsCfg.Credentials = credentials.NewStaticCredentials(
			values.accessKeyID,
			values.secretAccessKey,
			values.sessionToken)
	}
	awsCfg.MaxRetries = values.numMaxRetries
	if values.endpointURL != "" {
		awsCfg.EndpointResolver = endpoints.ResolverFunc(func(service, region string, opts ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
			return endpoints.ResolvedEndpoint{
				URL: values.endpointURL,
			}, nil
		})
	}

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
	endpointURL     string
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
		case connAttributeNameEndpointURL:
			s := v.(string)
			values.endpointURL = s
		}
	}
	return &values
}

type Connector struct {
	name       string
	svc        internal.SQSClient
	name2queue sync.Map
	loggerFunc jobworker.LoggerFunc
}

func (c *Connector) Name() string {
	return c.name
}

func (c *Connector) Subscribe(ctx context.Context, input *jobworker.SubscribeInput) (*jobworker.SubscribeOutput, error) {
	queue, err := c.resolveQueueAttributes(ctx, input.Queue)
	if err != nil {
		return nil, err
	}
	sub := internal.NewSubscription(queue, c.svc, c, input.Metadata)
	go sub.Start()
	return &jobworker.SubscribeOutput{
		Subscription: sub,
	}, nil

}

func (c *Connector) Enqueue(ctx context.Context, input *jobworker.EnqueueInput) (*jobworker.EnqueueOutput, error) {
	queue, err := c.resolveQueueAttributes(ctx, input.Queue)
	if err != nil {
		return nil, err
	}
	sendMessageInput := newSendMessageInput(input.Content, input.Metadata, input.CustomAttribute, queue)
	_, err = c.svc.SendMessageWithContext(ctx, sendMessageInput)
	if err != nil {
		return nil, err
	}
	return &jobworker.EnqueueOutput{}, nil
}

func (c *Connector) EnqueueBatch(ctx context.Context, input *jobworker.EnqueueBatchInput) (*jobworker.EnqueueBatchOutput, error) {

	queue, err := c.resolveQueueAttributes(ctx, input.Queue)
	if err != nil {
		return nil, err
	}
	var entries []*sqs.SendMessageBatchRequestEntry
	for _, entry := range input.Entries {
		entries = append(entries, newSendMessageBatchRequestEntry(entry.ID,
			entry.Content, entry.Metadata, entry.CustomAttribute, queue))
	}
	sqsOutput, err := c.svc.SendMessageBatchWithContext(ctx, &sqs.SendMessageBatchInput{
		Entries:  entries,
		QueueUrl: &queue.URL,
	})
	if err != nil {
		return nil, err
	}

	var output jobworker.EnqueueBatchOutput
	if sqsOutput != nil {
		for _, v := range sqsOutput.Successful {
			output.Successful = append(output.Successful, aws.StringValue(v.Id))
		}
		for _, v := range sqsOutput.Failed {
			output.Failed = append(output.Failed, aws.StringValue(v.Id))
		}
	}
	return &output, nil
}

func newSendMessageInput(content string,
	metadata map[string]string, attr map[string]*jobworker.CustomAttribute, queue *internal.QueueAttributes) *sqs.SendMessageInput {
	var input sqs.SendMessageInput
	input.MessageBody = aws.String(content)
	input.QueueUrl = aws.String(queue.URL)
	input.MessageAttributes = toSQSMessageAttributeValues(attr)
	if queue.IsFIFO() {
		// fifo only
		input.MessageGroupId = extractGroupID(metadata)
		input.MessageDeduplicationId = extractDeduplicationID(metadata)
		if input.MessageDeduplicationId == nil && queue.IsContentBasedDeduplication() {
			id := uuid.NewV4().String()
			input.MessageDeduplicationId = aws.String(id)
		}
	} else {
		// standard only
		input.DelaySeconds = extractDelaySeconds(metadata)
	}
	return &input
}

func newSendMessageBatchRequestEntry(id string, content string,
	metadata map[string]string, attr map[string]*jobworker.CustomAttribute, queue *internal.QueueAttributes) *sqs.SendMessageBatchRequestEntry {
	var entry sqs.SendMessageBatchRequestEntry
	entry.MessageBody = aws.String(content)
	entry.Id = aws.String(id)
	entry.MessageAttributes = toSQSMessageAttributeValues(attr)
	if queue.IsFIFO() {
		// fifo only
		entry.MessageGroupId = extractGroupID(metadata)
		entry.MessageDeduplicationId = extractDeduplicationID(metadata)
		if entry.MessageDeduplicationId == nil && queue.IsContentBasedDeduplication() {
			id := uuid.NewV4().String()
			entry.MessageDeduplicationId = aws.String(id)
		}
	} else {
		// standard only
		entry.DelaySeconds = extractDelaySeconds(metadata)
	}
	return &entry
}

func toSQSMessageAttributeValues(attr map[string]*jobworker.CustomAttribute) map[string]*sqs.MessageAttributeValue {
	if len(attr) == 0 {
		return nil
	}
	messageAttributes := make(map[string]*sqs.MessageAttributeValue)
	for k, v := range attr {
		messageAttributes[k] = &sqs.MessageAttributeValue{
			DataType:    aws.String(v.DataType),
			StringValue: aws.String(v.StringValue),
			BinaryValue: v.BinaryValue,
		}
	}
	return messageAttributes
}

func extractGroupID(meta map[string]string) *string {
	messageGroupId := meta[internal.MetadataKeyMessageGroupID]
	if messageGroupId == "" {
		messageGroupId = defaultMessageGroupID
	}
	return aws.String(messageGroupId)
}

func extractDeduplicationID(meta map[string]string) *string {
	if v := meta[internal.MetadataKeyMessageDeduplicationID]; v != "" {
		return aws.String(v)
	}
	return nil
}

func extractDelaySeconds(meta map[string]string) *int64 {
	v, ok := meta[internal.MetadataKeyMessageDelaySeconds]
	if ok {
		delaySeconds, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil
		}
		return aws.Int64(delaySeconds)
	}
	return nil
}

func (c *Connector) CompleteJob(ctx context.Context, input *jobworker.CompleteJobInput) (*jobworker.CompleteJobOutput, error) {
	queue, err := c.resolveQueueAttributes(ctx, input.Job.QueueName)
	if err != nil {
		return nil, err
	}
	_, err = c.svc.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &queue.URL,
		ReceiptHandle: extractReceiptHandle(input.Job),
	})
	if err != nil {
		return nil, err
	}
	return &jobworker.CompleteJobOutput{}, nil
}

func (c *Connector) FailJob(ctx context.Context, input *jobworker.FailJobInput) (*jobworker.FailJobOutput, error) {
	_, err := c.ChangeJobVisibility(ctx, &ChangeJobVisibilityInput{
		Job:               input.Job,
		VisibilityTimeout: 0,
	})
	if err != nil {
		return nil, err
	}
	return &jobworker.FailJobOutput{}, nil
}

func (c *Connector) Close() error {
	c.debug("no 'Close' available")
	return nil
}

func (c *Connector) SetLoggerFunc(f jobworker.LoggerFunc) {
	c.loggerFunc = f
}

func (c *Connector) resolveQueueAttributes(ctx context.Context, name string) (*internal.QueueAttributes, error) {

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

		v = &internal.QueueAttributes{
			Name:          name,
			URL:           aws.StringValue(urlOutput.QueueUrl),
			RawAttributes: attrsOutput.Attributes,
		}

		c.name2queue.Store(name, v)
	}

	return v.(*internal.QueueAttributes), nil
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
	queue, err := c.resolveQueueAttributes(ctx, input.Job.QueueName)
	if err != nil {
		return nil, err
	}
	_, err = c.svc.ChangeMessageVisibilityWithContext(ctx, &sqs.ChangeMessageVisibilityInput{
		ReceiptHandle:     extractReceiptHandle(input.Job),
		QueueUrl:          aws.String(queue.URL),
		VisibilityTimeout: aws.Int64(input.VisibilityTimeout),
	})
	if err != nil {
		return nil, err
	}
	return &ChangeJobVisibilityOutput{}, nil
}

func extractReceiptHandle(job *jobworker.Job) *string {
	msg, ok := job.Raw.(*sqs.Message)
	if !ok {
		return nil
	}
	return msg.ReceiptHandle
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
		return nil, err
	}
	return &CreateQueueOutput{}, nil
}

func (c *Connector) UpdateQueue(ctx context.Context, input *UpdateQueueInput) (*UpdateQueueOutput, error) {
	queue, err := c.resolveQueueAttributes(ctx, input.Name)
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
//	fromQueue, err := c.resolveQueueAttributes(ctx, input.From)
//	if err != nil {
//		// TODO
//		return nil, err
//	}
//
//	toQueue, err := c.resolveQueueAttributes(ctx, input.To)
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
//	messageDeduplicationId := reviveMsg.RawAttributes[sqs.MessageSystemAttributeNameMessageDeduplicationId]
//	messageGroupId := reviveMsg.RawAttributes[sqs.MessageSystemAttributeNameMessageGroupId]
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
