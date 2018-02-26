package collector

import (
	"encoding/json"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/examples/3-messaging"
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

func Run(brokers []string) {
	g := goka.DefineGroup(group,
		goka.Input(messaging.ReceivedStream, new(messaging.MessageCodec), collect),
		goka.Persist(new(MessageListCodec)),
	)
	if p, err := goka.NewProcessor(brokers, g); err != nil {
		panic(err)
	} else if err = p.Start(); err != nil {
		panic(err)
	}
}
