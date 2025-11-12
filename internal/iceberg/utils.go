package iceberg

import "net/url"

func cleanupURL(u *url.URL) {
	if u == nil {
		return
	}

	u.RawFragment = ""
	u.Fragment = ""
	u.RawQuery = ""
}
