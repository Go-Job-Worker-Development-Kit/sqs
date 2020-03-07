package internal

type QueueAttributes struct {
	Name          string
	URL           string
	RawAttributes map[string]*string
}

const (
	QueueAttributeKeyFifoQueue                 = "FifoQueue"
	QueueAttributeKeyContentBasedDeduplication = "ContentBasedDeduplication"
)
