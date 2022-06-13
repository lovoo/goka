package filter

import (
	"context"
	"github.com/lovoo/goka"
	messaging "github.com/lovoo/goka/examples/3-messaging"
	"github.com/lovoo/goka/examples/3-messaging/blocker"
	"github.com/lovoo/goka/examples/3-messaging/topicinit"
	"github.com/lovoo/goka/examples/3-messaging/translator"
	"strings"
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

func PrepareTopics(brokers []string) {
	topicinit.EnsureStreamExists(string(messaging.SentStream), brokers)

	// We refer to these tables, ensure that they exist initially also in the
	// case that the translator or blocker processors are not started
	for _, topicName := range []string{string(translator.Table), string(blocker.Table)} {
		topicinit.EnsureTableExists(topicName, brokers)
	}
}

func Run(ctx context.Context, brokers []string) func() error {
	return func() error {
		g := goka.DefineGroup(group,
			goka.Input(messaging.SentStream, new(messaging.MessageCodec), filter),
			goka.Output(messaging.ReceivedStream, new(messaging.MessageCodec)),
			goka.Join(blocker.Table, new(blocker.BlockValueCodec)),
			goka.Lookup(translator.Table, new(translator.ValueCodec)),
		)
		p, err := goka.NewProcessor(brokers, g)
		if err != nil {
			return err
		}

		return p.Run(ctx)
	}
}
