# aws-sqs-connector

A jobworker connector with Amazon SQS for [go-jwdk/jobworker](https://github.com/go-jwdk/jobworker) package.

## Requirements

Go 1.13+

## Installation

This package can be installed with the go get command:

```
$ go get -u github.com/go-jwdk/aws-sqs-connector
```

## Usage

```go
import "github.com/go-jwdk/jobworker"
import _ "github.com/go-jwdk/aws-sqs-connector/sqs"

conn, err := jobworker.Open("sqs", map[string]interface{}{
		"Region":          os.Getenv("REGION"),
	})
```

## Connection Params

| Key | Value | Required | Description |
|:---|:---|:---|:---|
|Region |string |true |The region to send requests to |
|AccessKeyID |string |false |AWS Access key ID |
|SecretAccessKey |string |false |AWS Secret Access Key |
|SessionToken |string |false |AWS Session Token |
|NumMaxRetries |int |false |The maximum number of times that a request will be retried for failures |


## Metadata String

### Received Job

| Key | Value | Description |Ref |
|:---|:---|:---|:---|
|MessageId |string |A unique identifier for the message. |- |
|ReceiptHandle |string |An identifier associated with the act of receiving the message. |- |
|MD5OfBody |string |An MD5 digest of the non-URL-encoded message body string. |
|MD5OfMessageAttributes |string|An MD5 digest of the non-URL-encoded message body string. |- |
|ApproximateReceiveCount |- |- |[API_ReceiveMessage.html](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html) |
|ApproximateFirstReceiveTimestamp |- |- |[API_ReceiveMessage.html](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html) |
|MessageDeduplicationId |- |- |[API_ReceiveMessage.html](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html) |
|MessageGroupId |- |- |[API_ReceiveMessage.html](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html) |
|SenderId |- |- |[API_ReceiveMessage.html](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html) |
|SentTimestamp |- |- |[API_ReceiveMessage.html](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html) |
|SequenceNumber |- |- |[API_ReceiveMessage.html](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html) |
|AWSTraceHeader |- |- |[API_ReceiveMessage.html](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html) |
|AWSTraceHeader |- |- |[API_ReceiveMessage.html](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html) |

## Enqueue Job

| Key | Value | Description |
|:---|:---|:---|
|MessageDeduplicationId |string |The token used for deduplication of sent messages. |
|MessageGroupId |string |The tag that specifies that a message belongs to a specific message group. |
|DelaySeconds |int64 |The length of time, in seconds, for which the delivery of all messages in the queue is delayed. |