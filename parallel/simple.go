package parallel

import (
	"sync"
	"sync/atomic"
	"time"
)

// 简单模式

type SimpleParallelProducerWorker func(idx int, cycle int) (interface{}, error)
type SimpleParallelConsumerWorker func(idx int, cycle int, data interface{}) error

type SimpleParallel struct {
	terminator bool

	pipeSize          int
	producerConNumber int
	consumerConNumber int

	producerWorker SimpleParallelProducerWorker

	consumerWorker SimpleParallelConsumerWorker

	lock *sync.RWMutex
}

func NewSimpleParallel(pipeSize, producerNumber, consumerNumber int,
	producerWorker SimpleParallelProducerWorker,
	consumerWorker SimpleParallelConsumerWorker) Parallel {
	inst := &SimpleParallel{
		terminator:        false,
		pipeSize:          pipeSize,
		producerConNumber: producerNumber,
		consumerConNumber: consumerNumber,
		producerWorker:    producerWorker,
		consumerWorker:    consumerWorker,
		lock:              &sync.RWMutex{},
	}
	return inst
}

func (this *SimpleParallel) IsTerminator() bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.terminator
}

func (this *SimpleParallel) setTerminator(terminator bool) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.terminator = terminator
}

func (this *SimpleParallel) Execute() error {
	pipe, pRet := this.producer()
	cRet := this.consumer(pipe)
	return this.collect(pRet, cRet)
}

func (this *SimpleParallel) producer() (<-chan interface{}, <-chan *PipeResult) {
	out := make(chan interface{}, this.pipeSize)
	retErr := make(chan *PipeResult, this.producerConNumber)

	var closed int32 = 0
	for i := 0; i < this.producerConNumber; i++ {
		go func(id int) {
			defer func() {
				v := atomic.AddInt32(&closed, 1)
				if v == int32(this.producerConNumber) {
					close(retErr)
					close(out)
				}
			}()

			beg := time.Now()
			var err error
			i := 0
			for ; ; i++ {
				if this.IsTerminator() {
					break
				}

				var result interface{}
				result, err = this.producerWorker(id, i)

				if err != nil {
					break
				}

				if result == nil {
					break
				}

				out <- result
			}

			retErr <- &PipeResult{
				Id:      id,
				Error:   err,
				Cycle:   i,
				BegTime: beg,
				EndTime: time.Now(),
			}
		}(i)
	}

	return out, retErr
}

func (this *SimpleParallel) consumer(in <-chan interface{}) <-chan *PipeResult {
	retErr := make(chan *PipeResult, this.consumerConNumber)

	var closed int32 = 0
	for i := 0; i < this.consumerConNumber; i++ {
		go func(id int) {
			defer func() {
				v := atomic.AddInt32(&closed, 1)
				if v == int32(this.consumerConNumber) {
					close(retErr)
				}
			}()

			beg := time.Now()
			var err error
			j := 0
			for data := range in {
				j++
				// 终止
				if this.IsTerminator() {
					break
				}

				err = this.consumerWorker(id, j, data)

				if err != nil {
					break
				}
			}

			retErr <- &PipeResult{
				Id:      id,
				Error:   err,
				Cycle:   j,
				BegTime: beg,
				EndTime: time.Now(),
			}
		}(i)
	}

	return retErr
}

func (this *SimpleParallel) collect(pRet <-chan *PipeResult, cRet <-chan *PipeResult) error {
	// 加一个producer的计数器
	l := this.producerConNumber + this.consumerConNumber
	var err error
	for {
		select {
		case r, ok := <-pRet:
			if ok {
				l--
				if r.Error != nil {
					err = r.Error
				}
			}
		case r, ok := <-cRet:
			if ok {
				l--
				if r.Error != nil {
					err = r.Error
				}
			}

		}
		if l <= 0 {
			break
		}
		// 有错误，则终止
		if err != nil {
			this.setTerminator(true)
		}
	}
	return err
}
