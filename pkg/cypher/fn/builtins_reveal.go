package fn

func init() {
	RegisterInvocation("reveal", evalReveal)
}

func evalReveal(ctx InvocationContext, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, nil
	}
	return ctx.Eval(0)
}
