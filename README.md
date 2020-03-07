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

| Key | Value | Description |
|:---|:---|:---|
|MessageId |? |? |
|ReceiptHandle |? |? |
|MD5OfBody |? |? |
|MD5OfMessageAttributes |? |? |
|ApproximateReceiveCount |string |? |
|ApproximateFirstReceiveTimestamp |? |? |
|MessageDeduplicationId |? |? |
|MessageGroupId |? |? |
|SenderId |? |? |
|SentTimestamp |string |? |
|SequenceNumber |? |? |
|AWSTraceHeader |? |? |

## Enqueue Job

| Key | Value | Description |
|:---|:---|:---|
|MessageDeduplicationId |string |The token used for deduplication of sent messages. |
|MessageGroupId |string |The tag that specifies that a message belongs to a specific message group. |
|DelaySeconds |int64 |The length of time, in seconds, for which the delivery of all messages in the queue is delayed. |