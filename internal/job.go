package internal

import (
	"github.com/go-job-worker-development-kit/jobworker"
)

type Job struct {
	JobID           string
	Class           string
	ReceiptID       string
	Args            string
	DeduplicationID string
	GroupID         string
	InvisibleUntil  int64
	RetryCount      int64
	EnqueueAt       int64
}

func (j *Job) ToJob(queue string, conn jobworker.Connector) *jobworker.Job {
	job := jobworker.NewJob(
		queue,
		j.JobID,
		j.Class,
		j.ReceiptID,
		j.Args,
		j.DeduplicationID,
		j.GroupID,
		j.InvisibleUntil,
		j.RetryCount,
		j.EnqueueAt,
		conn,
	)
	return job
}
