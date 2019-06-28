package reads

import "github.com/influxdata/flux"

func IsDone(tbl flux.Table) bool {
	t := tbl.(interface {
		isDone() bool
	})
	return t.isDone()
}
