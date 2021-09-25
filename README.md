# aws-sqs-connector ![](https://github.com/go-jwdk/aws-sqs-connector/workflows/Go/badge.svg)

A jobworker connector with Amazon SQS for [go-jwdk/jobworker](https://github.com/go-jwdk/jobworker) package.

## Requirements

Go 1.13+

## Installation

This package can be installed with the go get command:

```
$ go get -u github.com/go-jwdk/aws-sqs-connector
```

## Usage

### Basic

```go
package main

import (
	jw "github.com/go-jwdk/jobworker"
    _ "github.com/go-jwdk/aws-sqs-connector"
)

func main() {
    conn, err := jw.Open("sqs", map[string]interface{}{
        "Region": "us-east-1",
    })
    
    ...
}
```

### Using SQS instances directly

If you want to use more SQS advanced options.

```go
package main

import (
	...

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"

	sqsc "github.com/go-jwdk/aws-sqs-connector"
	"github.com/go-jwdk/jobworker"
)

func main() {
	awsCfg := &aws.Config{
		Region:     aws.String("us-east-1"),
		MaxRetries: aws.Int(5),
	}
	sess, err := session.NewSession(&awsCfg)
	if err != nil {
		panic(err)
	}
	svc := sqs.New(sess)

	conn, err := sqsc.OpenWithSQS(svc)
	
	...
}
```

## Connection Params

| Key | Value | Required | Description |
|:---|:---|:---|:---|
|Region |string |true |The region to send requests to |
|AccessKeyID |string |false |AWS Access key ID |
|SecretAccessKey |string |false |AWS Secret Access Key |
|SessionToken |string |false |AWS Session Token |
|NumMaxRetries |int |false |The maximum number of times that a request will be retried for failures |
|EndpointURL |string |false |An optional endpoint URL (hostname only or fully qualified URI) |

## Metadata String

### Received Job

| Key | Value | Description | Ref |
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

### Enqueue Job

| Key | Value | Description |
|:---|:---|:---|
|MessageDeduplicationId |string |（fifo only）The token used for deduplication of sent messages. |
|MessageGroupId |string |（fifo only）The tag that specifies that a message belongs to a specific message group. |
|MessageDelaySeconds |int64 |（standard only）The length of time, in seconds, for which the delivery of all messages in the queue is delayed. |

### Subscribe Queue

| Key | Value | Description |
|:---|:---|:---|
|PollingInterval |int64 |If there is no job, wait until the polling interval time (in seconds). |
|VisibilityTimeout |int64 |The duration (in seconds) that the received messages are hidden from subsequent retrieve requests after being retrieved by a ReceiveMessage request. |
|WaitTimeSeconds |int64 |The duration (in seconds) for which the call waits for a message to arrive in the queue before returning. |
|MaxNumberOfJobs |int64 |The maximum number of jobs to return. |
