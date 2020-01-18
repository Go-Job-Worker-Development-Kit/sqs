# aws-sqs-connector

A jobworker connector for [go-job-worker-development-kit/jobworker](https://github.com/go-job-worker-development-kit/jobworker) package.

## Requirements

Go 1.13+

## Installation

This package can be installed with the go get command:

```
$ $ go get -u github.com/go-job-worker-development-kit/aws-sqs-connector
```

## Usage

```go
import "github.com/go-job-worker-development-kit/jobworker"
import _ "github.com/go-job-worker-development-kit/aws-sqs-connector/sqs"

conn, err := jobworker.Open("sqs", map[string]interface{}{
		"Region":          os.Getenv("REGION"),
		"AccessKeyID":     os.Getenv("ACCESS_KEY_ID"),
		"SecretAccessKey": os.Getenv("SECRET_ACCESS_KEY"),
	})
```