package internal

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
)

func TestQueueAttributes_IsFIFO(t *testing.T) {
	type fields struct {
		RawAttributes map[string]*string
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "is FIFO",
			fields: fields{
				RawAttributes: map[string]*string{
					QueueAttributeKeyFifoQueue: aws.String("true"),
				},
			},
			want: true,
		},
		{
			name: "is not FIFO",
			fields: fields{
				RawAttributes: map[string]*string{
					QueueAttributeKeyFifoQueue: aws.String("false"),
				},
			},
			want: false,
		},
		{
			name: "is not FIFO",
			fields: fields{
				RawAttributes: map[string]*string{},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &QueueAttributes{
				RawAttributes: tt.fields.RawAttributes,
			}
			if got := q.IsFIFO(); got != tt.want {
				t.Errorf("QueueAttributes.IsFIFO() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueueAttributes_IsContentBasedDeduplication(t *testing.T) {
	type fields struct {
		RawAttributes map[string]*string
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "is ContentBasedDeduplication",
			fields: fields{
				RawAttributes: map[string]*string{
					QueueAttributeKeyContentBasedDeduplication: aws.String("true"),
				},
			},
			want: true,
		},
		{
			name: "is not ContentBasedDeduplication",
			fields: fields{
				RawAttributes: map[string]*string{
					QueueAttributeKeyContentBasedDeduplication: aws.String("false"),
				},
			},
			want: false,
		},
		{
			name: "is not ContentBasedDeduplication",
			fields: fields{
				RawAttributes: map[string]*string{},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &QueueAttributes{
				RawAttributes: tt.fields.RawAttributes,
			}
			if got := q.IsContentBasedDeduplication(); got != tt.want {
				t.Errorf("QueueAttributes.IsContentBasedDeduplication() = %v, want %v", got, tt.want)
			}
		})
	}
}
