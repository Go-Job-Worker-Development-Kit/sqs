package internal

type Queue struct {
	URL        string
	Attributes map[string]*string
}

const (
	QueueAttributeKeyFifoQueue                 = "FifoQueue"
	QueueAttributeKeyContentBasedDeduplication = "ContentBasedDeduplication"
)
