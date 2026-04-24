package fn

func init() {
	Register("reveal", evalReveal)
}

func evalReveal(ctx Context, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, nil
	}
	return ctx.Eval(args[0])
}
