package sqs

import (
	"context"
	"errors"
	"strconv"
	"sync"

	"github.com/go-job-worker-development-kit/aws-sqs-connector/internal"
	"github.com/go-job-worker-development-kit/jobworker"

	uuid "github.com/satori/go.uuid"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func Open(attrs map[string]interface{}) (*Connector, error) {
	values := connAttrsToValues(attrs)
	values.applyDefaultValues()

	awsCfg := &aws.Config{
		Region: values.region,
		Credentials: credentials.NewStaticCredentials(
			values.accessKeyID,
			values.secretAccessKey,
			values.sessionToken),
		MaxRetries: values.numMaxRetries,
	}

	sess, err := session.NewSession(awsCfg)
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

func init() {
	jobworker.Register("sqs", &Provider{})
}

type Provider struct {
}

func (Provider) Open(attrs map[string]interface{}) (jobworker.Connector, error) {
	return Open(attrs)
}

const (
	logPrefix = "[sqs-connector]"

	connName = "sqs"

	connAttributeNameAwsRegion          = "Region"
	connAttributeNameAwsAccessKeyID     = "AccessKeyID"
	connAttributeNameAwsSecretAccessKey = "SecretAccessKey"
	connAttributeNameAwsSessionToken    = "SessionToken"
	connAttributeNameNumMaxRetries      = "NumMaxRetries"

	messageAttributeNameJobClass = "JobClass"
)

type values struct {
	region          *string
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
	numMaxRetries   *int
}

func (v *values) applyDefaultValues() {
	if v.numMaxRetries == nil {
		i := 3
		v.numMaxRetries = &i
	}
}

func connAttrsToValues(attrs map[string]interface{}) *values {
	var values values
	for k, v := range attrs {
		switch k {
		case connAttributeNameAwsRegion:
			s := v.(string)
			values.region = &s
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

func (c *Connector) GetName() string {
	return c.name
}

func (c *Connector) CreateQueue(ctx context.Context, input *jobworker.CreateQueueInput, opts ...func(*jobworker.Option)) (*jobworker.CreateQueueOutput, error) {

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
	return &jobworker.CreateQueueOutput{}, nil
}

func (c *Connector) UpdateQueue(ctx context.Context, input *jobworker.UpdateQueueInput, opts ...func(*jobworker.Option)) (*jobworker.UpdateQueueOutput, error) {
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
	return &jobworker.UpdateQueueOutput{}, nil
}

func (c *Connector) Close() error {
	c.debug("no 'Close' available")
	return nil
}

func (c *Connector) SetLoggerFunc(f jobworker.LoggerFunc) {
	c.loggerFunc = f
}

func (c *Connector) Subscribe(ctx context.Context, input *jobworker.SubscribeInput, opts ...func(*jobworker.Option)) (*jobworker.SubscribeOutput, error) {
	queue, err := c.resolveQueue(ctx, input.Queue)
	if err != nil {
		// TODO
		return nil, err
	}
	result, err := c.svc.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl: aws.String(queue.URL),
	})
	if err != nil {
		return nil, err
	}
	if len(result.Messages) == 0 {
		return &jobworker.ReceiveJobsOutput{}, nil
	}

	var jobs []*jobworker.Job
	for _, msg := range result.Messages {

		// Not use props below:
		// - sqs.MessageSystemAttributeNameSenderId
		// - sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp
		// - sqs.MessageSystemAttributeNameAwstraceHeader
		// - sqs.MessageSystemAttributeNameSequenceNumber

		sentTimestamp := msg.Attributes[sqs.MessageSystemAttributeNameSentTimestamp]
		approximateReceiveCount := msg.Attributes[sqs.MessageSystemAttributeNameApproximateReceiveCount]
		messageDeduplicationId := msg.Attributes[sqs.MessageSystemAttributeNameMessageDeduplicationId]
		messageGroupId := msg.Attributes[sqs.MessageSystemAttributeNameMessageGroupId]

		class := msg.MessageAttributes[messageAttributeNameJobClass]

		retryCount, err := strconv.ParseInt(aws.StringValue(approximateReceiveCount), 10, 64)
		if err != nil {
			c.debug("could not parse approximateReceiveCount:", approximateReceiveCount, err)
		}

		enqueueAt, err := strconv.ParseInt(aws.StringValue(sentTimestamp), 10, 64)
		if err != nil {
			c.debug("could not parse sentTimestamp:", sentTimestamp, err)
		}

		job := &internal.Job{
			JobID:           aws.StringValue(msg.MessageId),
			Class:           aws.StringValue(class.StringValue),
			ReceiptID:       aws.StringValue(msg.ReceiptHandle),
			Args:            aws.StringValue(msg.Body),
			DeduplicationID: aws.StringValue(messageDeduplicationId),
			GroupID:         aws.StringValue(messageGroupId),
			RetryCount:      retryCount,
			EnqueueAt:       enqueueAt,
		}
		jobs = append(jobs, job.ToJob(input.Queue, c))

	}

	for _, job := range jobs {
		ch <- job
	}

	return &jobworker.ReceiveJobsOutput{
		NoJob: len(jobs) == 0,
	}, nil

}

func (c *Connector) RedriveJob(ctx context.Context, input *jobworker.RedriveJobInput, opts ...func(*jobworker.Option)) (*jobworker.RedriveJobOutput, error) {
	fromQueue, err := c.resolveQueue(ctx, input.From)
	if err != nil {
		// TODO
		return nil, err
	}

	toQueue, err := c.resolveQueue(ctx, input.To)
	if err != nil {
		// TODO
		return nil, err
	}

	waitTimeSeconds := int64(2)
	maxNumberOfMessages := int64(10)
	visibilityTimeout := int64(20)

	var reviveMsg *sqs.Message

L:
	for {

		select {
		case <-ctx.Done():
			// TODO
			return nil, errors.New("timeout")
		default:
			result, err := c.svc.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
				AttributeNames: []*string{
					aws.String(sqs.QueueAttributeNameAll),
				},
				MessageAttributeNames: []*string{
					aws.String(sqs.QueueAttributeNameAll),
				},
				QueueUrl:            aws.String(fromQueue.URL),
				MaxNumberOfMessages: aws.Int64(maxNumberOfMessages),
				VisibilityTimeout:   aws.Int64(visibilityTimeout),
				WaitTimeSeconds:     aws.Int64(waitTimeSeconds),
			})
			if err != nil {
				// TODO
				return nil, err
			}
			for _, msg := range result.Messages {
				if aws.StringValue(msg.MessageId) == input.Target {
					reviveMsg = msg
					break L
				}
			}
		}
	}

	messageDeduplicationId := reviveMsg.Attributes[sqs.MessageSystemAttributeNameMessageDeduplicationId]
	messageGroupId := reviveMsg.Attributes[sqs.MessageSystemAttributeNameMessageGroupId]
	class := reviveMsg.MessageAttributes[messageAttributeNameJobClass]

	_, err = c.svc.SendMessageWithContext(ctx, &sqs.SendMessageInput{
		DelaySeconds: aws.Int64(input.DelaySeconds),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			messageAttributeNameJobClass: {
				DataType:    aws.String("String"),
				StringValue: class.StringValue,
			},
		},
		MessageBody:             reviveMsg.Body,
		MessageDeduplicationId:  messageDeduplicationId,
		MessageGroupId:          messageGroupId,
		MessageSystemAttributes: map[string]*sqs.MessageSystemAttributeValue{},
		QueueUrl:                aws.String(toQueue.URL),
	})
	if err != nil {
		// TODO
		return nil, err
	}

	_, err = c.svc.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(fromQueue.URL),
		ReceiptHandle: reviveMsg.ReceiptHandle,
	})
	if err != nil {
		// TODO
		return nil, err
	}
	return &jobworker.RedriveJobOutput{}, nil

}

func (c *Connector) EnqueueJob(ctx context.Context, input *jobworker.EnqueueJobInput, opts ...func(*jobworker.Option)) (*jobworker.EnqueueJobOutput, error) {

	queue, err := c.resolveQueue(ctx, input.Queue)
	if err != nil {
		// TODO
		return nil, err
	}
	output, err := c.svc.SendMessageWithContext(ctx, newSendMessageInput(input.Payload, queue))
	if err != nil {
		return nil, err
	}
	return &jobworker.EnqueueJobOutput{
		JobID: aws.StringValue(output.MessageId),
	}, nil
}

func newSendMessageInput(payload *jobworker.Payload, queue *internal.Queue) *sqs.SendMessageInput {

	input := &sqs.SendMessageInput{
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			messageAttributeNameJobClass: {
				DataType:    aws.String("String"),
				StringValue: aws.String(payload.Class),
			},
		},
		MessageBody: aws.String(payload.Args),
		QueueUrl:    aws.String(queue.URL),
	}

	if v, ok := queue.Attributes["FifoQueue"]; ok && aws.StringValue(v) == "true" {
		// fifo only
		messageGroupId := payload.GroupID
		if messageGroupId == "" {
			messageGroupId = "default"
		}
		input.MessageGroupId = aws.String(messageGroupId)

		if payload.DeduplicationID != "" {
			input.MessageDeduplicationId = aws.String(payload.DeduplicationID)
		}

		if v, ok := queue.Attributes["ContentBasedDeduplication"]; ok && aws.StringValue(v) != "true" {
			if input.MessageDeduplicationId == nil {
				id := uuid.NewV4().String()
				input.MessageDeduplicationId = &id
			}
		}

	} else {
		// standard only
		input.DelaySeconds = aws.Int64(payload.DelaySeconds)
	}

	return input
}

func (c *Connector) EnqueueJobBatch(ctx context.Context, input *jobworker.EnqueueJobBatchInput, opts ...func(*jobworker.Option)) (*jobworker.EnqueueJobBatchOutput, error) {

	queue, err := c.resolveQueue(ctx, input.Queue)
	if err != nil {
		// TODO
		return nil, err
	}

	var entries []*sqs.SendMessageBatchRequestEntry
	for k, v := range input.Id2Payload {
		entries = append(entries, newSendMessageBatchRequestEntry(k, v))
	}

	sqsOutput, err := c.svc.SendMessageBatchWithContext(ctx, &sqs.SendMessageBatchInput{
		Entries:  entries,
		QueueUrl: &queue.URL,
	})

	var output jobworker.EnqueueJobBatchOutput
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

func newSendMessageBatchRequestEntry(id string, payload *jobworker.Payload) *sqs.SendMessageBatchRequestEntry {
	return &sqs.SendMessageBatchRequestEntry{
		Id:           aws.String(id),
		DelaySeconds: aws.Int64(payload.DelaySeconds),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			messageAttributeNameJobClass: {
				DataType:    aws.String("String"),
				StringValue: aws.String(payload.Class),
			},
		},
		MessageBody:             aws.String(payload.Args),
		MessageDeduplicationId:  aws.String(payload.DeduplicationID),
		MessageGroupId:          aws.String(payload.GroupID),
		MessageSystemAttributes: map[string]*sqs.MessageSystemAttributeValue{},
	}
}

func (c *Connector) CompleteJob(ctx context.Context, input *jobworker.CompleteJobInput, opts ...func(*jobworker.Option)) (*jobworker.CompleteJobOutput, error) {
	queue, err := c.resolveQueue(ctx, input.Job.Queue)
	if err != nil {
		// TODO
		return nil, err
	}
	_, err = c.svc.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &queue.URL,
		ReceiptHandle: aws.String(input.Job.ReceiptID),
	})
	if err != nil {
		// TODO handle already delete error
		return nil, err
	}
	return &jobworker.CompleteJobOutput{}, nil
}

func (c *Connector) FailJob(ctx context.Context, input *jobworker.FailJobInput, opts ...func(*jobworker.Option)) (*jobworker.FailJobOutput, error) {
	_, err := c.ChangeJobVisibility(ctx, &jobworker.ChangeJobVisibilityInput{
		Job:               input.Job,
		VisibilityTimeout: 0,
	})
	return &jobworker.FailJobOutput{}, err
}

func (c *Connector) ChangeJobVisibility(ctx context.Context, input *jobworker.ChangeJobVisibilityInput, opts ...func(*jobworker.Option)) (*jobworker.ChangeJobVisibilityOutput, error) {
	queue, err := c.resolveQueue(ctx, input.Job.Queue)
	if err != nil {
		// TODO
		return nil, err
	}
	_, err = c.svc.ChangeMessageVisibilityWithContext(ctx, &sqs.ChangeMessageVisibilityInput{
		ReceiptHandle:     &input.Job.ReceiptID,
		QueueUrl:          &queue.URL,
		VisibilityTimeout: &input.VisibilityTimeout,
	})
	return &jobworker.ChangeJobVisibilityOutput{}, nil
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
