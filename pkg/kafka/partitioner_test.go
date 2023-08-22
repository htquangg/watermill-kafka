package kafka

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

func TestHashPartitioner(t *testing.T) {
	partitioner := NewHashPartitioner()

	choice, err := partitioner.Partition(message.NewMessage("e54a0566-f3fd-4644-ae5d-03ee0978203c", message.Payload{}), 1)
	if err != nil {
		t.Error(partitioner, err)
	}
	if choice != 0 {
		t.Error("Returned non-zero partition when only one available.")
	}

	for i := 1; i < 50; i++ {
		choice, err := partitioner.Partition(message.NewMessage(watermill.NewUUID(), message.Payload{}), 50)
		if err != nil {
			t.Error(partitioner, err)
		}
		if choice < 0 || choice >= 50 {
			t.Error("Returned partition", choice, "outside of range for nil key.")
		}
	}
}
