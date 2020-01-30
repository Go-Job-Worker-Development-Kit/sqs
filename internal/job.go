package internal

import (
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/go-job-worker-development-kit/jobworker"
)

type Job struct {
	JobID    string
	Class    string
	Args     string
	Metadata map[string]string
}

func (j *Job) ToJob(queue string, msg *sqs.Message, conn jobworker.Connector) *jobworker.Job {
	return jobworker.NewJob(
		queue,
		msg.
			j.JobID,
		j.Class,
		j.Args,
		j.Metadata,
		conn,
	)
}
