package tester

type client struct {
	clientID      string
	consumerGroup *consumerGroup
	consumer      *consumerMock
}

func (c *client) waitStartup() {
	if c.consumerGroup != nil {
		c.consumerGroup.waitRunning()
	}

	c.consumer.waitRequiredConsumersStartup()
}

func (c *client) requireConsumer(topic string) {
	c.consumer.requirePartConsumer(topic)
}

func (c *client) catchup() int {
	var catchup int
	if c.consumerGroup != nil {
		catchup += c.consumerGroup.catchupAndWait()
	}

	catchup += c.consumer.catchup()

	return catchup
}
