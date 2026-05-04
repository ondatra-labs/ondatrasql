package a

type runtime struct{}

func (runtime) RunPush(ctx, target, rows, num, kind, key, cols, args, http any) (any, error) {
	return nil, nil
}
func (runtime) RunPushFinalize(ctx, target, succeeded, failed, http any) error { return nil }
func (runtime) RunPushPoll(ctx, target, jobRef, http any) (any, any, error)    { return nil, nil, nil }

func httpConfigFromLib(api, ctx, dir any) any { return nil }

func goodPush() {
	rt := runtime{}
	_, _ = rt.RunPush(nil, nil, nil, 0, nil, nil, nil, nil, httpConfigFromLib(nil, nil, nil))
}

func goodFinalize() {
	rt := runtime{}
	_ = rt.RunPushFinalize(nil, nil, 0, 0, httpConfigFromLib(nil, nil, nil))
}

func badNoHTTPConfig() {
	rt := runtime{}
	_, _ = rt.RunPush(nil, nil, nil, 0, nil, nil, nil, nil, nil) // want `lacks an httpConfigFromLib`
}

func badFinalizeMissing() {
	rt := runtime{}
	_ = rt.RunPushFinalize(nil, nil, 0, 0, nil) // want `lacks an httpConfigFromLib`
}

func unrelated() {
	// Method name doesn't start with RunPush — out of scope.
	_ = "RunQuery and friends ignored"
}
