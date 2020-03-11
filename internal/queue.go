package internal

import "github.com/aws/aws-sdk-go/aws"

type QueueAttributes struct {
	Name          string
	URL           string
	RawAttributes map[string]*string
}

func (q *QueueAttributes) IsFIFO() bool {
	v, ok := q.RawAttributes[QueueAttributeKeyFifoQueue]
	return ok && aws.StringValue(v) == "true"
}

func (q *QueueAttributes) IsContentBasedDeduplication() bool {
	v, ok := q.RawAttributes[QueueAttributeKeyContentBasedDeduplication]
	return ok && aws.StringValue(v) != "true"
}

const (
	QueueAttributeKeyFifoQueue                 = "FifoQueue"
	QueueAttributeKeyContentBasedDeduplication = "ContentBasedDeduplication"
)
