package iceberg

import (
	"strings"

	"github.com/apache/iceberg-go"
)

func ParseProperties(ss []string) iceberg.Properties {
	var res = make(map[string]string)

	for _, s := range ss {
		k, v, _ := strings.Cut(s, "=")
		res[k] = v
	}

	return res
}
