package parallel

import (
	"fmt"
	"time"
)

type Parallel interface {
	Execute() error
}

type PipeResult struct {
	Id      int
	Error   error
	Cycle   int
	BegTime time.Time
	EndTime time.Time
}

func (p *PipeResult) String() string {
	cycle := p.Cycle
	if cycle == 0 {
		cycle = 1
	}
	used := p.EndTime.Sub(p.BegTime)
	return fmt.Sprintf("id=[%v] cycle=[%v] beg=[%v] end=[%v] used=[%v] avg=[%v] err=[%v]", p.Id, p.Cycle, p.BegTime, p.EndTime, used, used/time.Duration(cycle), p.Error)
}
