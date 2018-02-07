package filter

import (
	"strings"

	"github.com/lovoo/goka"
	messaging "github.com/lovoo/goka/examples/3-messaging"
	"github.com/lovoo/goka/examples/3-messaging/blocker"
	"github.com/lovoo/goka/examples/3-messaging/translator"
)

var (
	group goka.Group = "message_filter"
)

func shouldDrop(ctx goka.Context) bool {
	v := ctx.Join(blocker.Table)
	return v != nil && v.(*blocker.BlockValue).Blocked
}

func filter(ctx goka.Context, msg interface{}) {
	if shouldDrop(ctx) {
		return
	}
	m := translate(ctx, msg.(*messaging.Message))
	ctx.Emit(messaging.ReceivedStream, m.To, m)
}

func translate(ctx goka.Context, m *messaging.Message) *messaging.Message {
	words := strings.Split(m.Content, " ")
	for i, w := range words {
		if tw := ctx.Lookup(translator.Table, w); tw != nil {
			words[i] = tw.(string)
		}
	}
	return &messaging.Message{
		From:    m.From,
		To:      m.To,
		Content: strings.Join(words, " "),
	}
}

func Run(brokers []string) {
	g := goka.DefineGroup(group,
		goka.Input(messaging.SentStream, new(messaging.MessageCodec), filter),
		goka.Output(messaging.ReceivedStream, new(messaging.MessageCodec)),
		goka.Join(blocker.Table, new(blocker.BlockValueCodec)),
		goka.Lookup(translator.Table, new(translator.ValueCodec)),
	)
	if p, err := goka.NewProcessor(brokers, g); err != nil {
		panic(err)
	} else if err = p.Start(); err != nil {
		panic(err)
	}
}
