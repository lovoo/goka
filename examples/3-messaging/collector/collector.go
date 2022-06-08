package collector

import (
	"context"
	"encoding/json"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/examples/3-messaging"
	"github.com/lovoo/goka/examples/3-messaging/topic"
	"sync"
)

const maxMessages = 5

var (
	group goka.Group = "collector"
	Table goka.Table = goka.GroupTable(group)
)

type MessageListCodec struct{}

func (c *MessageListCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *MessageListCodec) Decode(data []byte) (interface{}, error) {
	var m []messaging.Message
	err := json.Unmarshal(data, &m)
	return m, err
}

func collect(ctx goka.Context, msg interface{}) {
	var ml []messaging.Message
	if v := ctx.Value(); v != nil {
		ml = v.([]messaging.Message)
	}

	m := msg.(*messaging.Message)
	ml = append(ml, *m)

	if len(ml) > maxMessages {
		ml = ml[len(ml)-maxMessages:]
	}
	ctx.SetValue(ml)
}

func Run(ctx context.Context, brokers []string, initialized *sync.WaitGroup) func() error {
	return func() error {
		topic.EnsureStreamExists(string(messaging.ReceivedStream), brokers)

		g := goka.DefineGroup(group,
			goka.Input(messaging.ReceivedStream, new(messaging.MessageCodec), collect),
			goka.Persist(new(MessageListCodec)),
		)
		p, err := goka.NewProcessor(brokers, g)
		if err != nil {
			// we have to signal done here so other Goroutines of the errgroup
			// can continue execution
			initialized.Done()
			return err
		}

		initialized.Done()
		initialized.Wait()

		return p.Run(ctx)
	}
}
