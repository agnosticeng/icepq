package show

import (
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/agnosticeng/objstr"
	"github.com/hamba/avro/v2/ocf"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "show",
		Usage: "show <path>",
		Action: func(ctx *cli.Context) error {
			var (
				os   = objstr.FromContext(ctx.Context)
				path = ctx.Args().Get(0)
			)

			u, err := url.Parse(path)

			if err != nil {
				return err
			}

			r, err := os.Reader(ctx.Context, u)

			if err != nil {
				return err
			}

			defer r.Close()

			dec, err := ocf.NewDecoder(r)

			if err != nil {
				return err
			}

			var content = struct {
				Metadata map[string][]byte `json:"metadata"`
				Records  []map[string]any  `json:"records"`
			}{}

			content.Metadata = dec.Metadata()

			for dec.HasNext() {
				var record map[string]any

				if err := dec.Decode(&record); err != nil {
					return err
				}

				content.Records = append(content.Records, record)
			}

			if err := dec.Error(); err != nil {
				return err
			}

			js, _ := json.MarshalIndent(content, "", "    ")
			fmt.Println(string(js))
			return nil
		},
	}
}
