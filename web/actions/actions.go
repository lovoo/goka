package actions

import "context"

type funcActor struct {
	description string
	actor       func(ctx context.Context, value string) error
}

func FuncActor(description string, actor func(ctx context.Context, value string) error) Actor {
	return &funcActor{
		description: description,
		actor:       actor,
	}
}

func (fa *funcActor) RunAction(ctx context.Context, value string) error {
	return fa.actor(ctx, value)
}
func (fa *funcActor) Description() string {
	return fa.description
}
