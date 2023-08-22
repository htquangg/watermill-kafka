package kafka

import (
	"hash"
	"hash/fnv"

	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Partitioner interface {
	Partition(msg *message.Message, numPartitions int32) (int32, error)
}

type hashPartitioner struct {
	random Partitioner
	hasher hash.Hash32
}

func NewHashPartitioner() Partitioner {
	p := new(hashPartitioner)
	p.hasher = fnv.New32a()
	return p
}

func (p *hashPartitioner) Partition(msg *message.Message, numPartitions int32) (int32, error) {
	// get raw value partition key
	key := msg.Metadata.Get(msg.UUID)

	bytes, err := sarama.ByteEncoder(key).Encode()
	if err != nil {
		return -1, err
	}
	p.hasher.Reset()
	_, err = p.hasher.Write(bytes)
	if err != nil {
		return -1, err
	}
	var partition int32
	partition = int32(p.hasher.Sum32()) % numPartitions
	if partition < 0 {
		partition = -partition
	}
	return partition, nil
}
