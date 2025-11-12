package utils

import (
	"context"
	"net/url"

	"github.com/agnosticeng/objstr"
)

func InjectURLObjectStore(ctx context.Context, s string) context.Context {
	u, err := url.Parse(s)
	if err != nil {
		return ctx
	}

	if len(u.Fragment) == 0 {
		return ctx
	}

	os, err := objstr.NewObjectStoreFromURL(ctx, u)
	if err != nil {
		return ctx
	}

	return objstr.NewContext(ctx, os)
}

func StripURLFragment(s string) string {
	u, err := url.Parse(s)
	if err != nil {
		return s
	}
	u.RawFragment = ""
	u.Fragment = ""
	return u.String()
}
