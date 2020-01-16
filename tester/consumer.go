package tester

/*
type SaramaConsumer struct {
	// tt                *Tester2
	expectedConsumers map[string]*PartitionConsumer
}

func NewSaramaConsumer(tt *Tester2) *SaramaConsumer {
	return &SaramaConsumer{
		tt:                tt,
		expectedConsumers: make(map[string]*PartitionConsumer),
	}
}

func (sc *SaramaConsumer) Topics() ([]string, error) {
	return nil, fmt.Errorf("listing topics not supported by the mock")
}
func (sc *SaramaConsumer) Partitions(topic string) ([]int32, error) {
	return nil, fmt.Errorf("listing partitions not supported by mock")
}

func (sc *SaramaConsumer) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	if partition != 0 {
		return nil, fmt.Errorf("Can only consume from partition 0")
	}

	cons := sc.expectedConsumers[topic]
	if cons == nil {
		return nil, fmt.Errorf("Unexpected Partition Consumer requested for topic %s", topic)
	}

	if cons.inUse {
		return nil, fmt.Errorf("Received double consume-partition for topic %s")
	}
	cons.startFrom(offset)

	return cons, nil
}

func (sc *SaramaConsumer) ExpectConsume(topic string) error {
	if sc.expectedConsumers[topic] != nil {
		return fmt.Errorf("Already expecting consumer")
	}
	sc.expectedConsumers[topic] = NewPartitionConsumer(sc.tt.topicQueues[topic])
	return nil
}

func (sc *SaramaConsumer) HighWaterMarks() map[string]map[int32]int64 {
	return nil
}
func (sc *SaramaConsumer) Close() error {
	return nil
}

type PartitionConsumer struct {
	inUse bool
	queue    *queue2
	messages chan *sarama.ConsumerMessage
	errors   chan *sarama.ConsumerError
}

func NewPartitionConsumer(queue *queue2) *PartitionConsumer {
	return &PartitionConsumer{
		queue: queue,
	}
}

func (pc *PartitionConsumer) startFrom(offset int64) {

	pc.messages = make(chan *sarama.ConsumerMessage)
	pc.errors = make(chan *sarama.ConsumerError)
	pc.inUse = true
	// TODO: set the offset and start consuming from the queue
}

func (pc *PartitionConsumer) AsyncClose() {
}

func (pc *PartitionConsumer) Close() error {
	close(pc.messages)
	close(pc.errors)

	pc.inUse = false

	return nil
}

func (pc *PartitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return pc.messages
}

func (pc *PartitionConsumer) Errors() <-chan *sarama.ConsumerError {
	return pc.errors
}

func (pc *PartitionConsumer) HighWaterMarkOffset() int64 {
	// TODO: getter for hwm to have a lock
	return pc.queue.hwm
}
*/
