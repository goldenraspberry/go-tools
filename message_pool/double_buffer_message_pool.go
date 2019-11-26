package message_pool

import (
	"fmt"
	"sync"
	"time"
)

type MessagePool interface {
	PublishMessage(interface{})

	SetLogRecord(LogRecord)
}

type MessageReceiver func(interface{}) error

type LogRecord func(string)

const (
	_POS_FLAG_0 = 0
	_POS_FLAG_1 = 1
)

// 双buffer缓冲池
type DoubleBufferMessagePool struct {
	messageChannelLength int
	messageChannel       chan interface{} // 消息管道
	producerBufferPool   *[]interface{}   // 生产缓冲区指针
	consumerBufferPool   *[]interface{}   // 消费缓冲区指针
	producerBufferVolume uint             // 生产缓冲区当前数据量
	consumerBufferVolume uint             // 消费缓冲区当前数据量
	posFlag              uint8            // 缓冲区标记
	bufferPool           [2][]interface{} // 双缓冲区
	retry                int              // 重试次数
	messageReceiver      MessageReceiver  // 监控处理函数
	producerLock         sync.Mutex
	consumerLock         sync.Mutex
	switchRepeatSleep    time.Duration
	logRecord            LogRecord
	closed               bool
}

func NewDoubleBufferMessagePool(receiver MessageReceiver, logger LogRecord, channelLength int, retry int, switchSleep time.Duration) MessagePool {
	d := &DoubleBufferMessagePool{
		logRecord:            logger,
		messageChannelLength: channelLength,
		messageReceiver:      receiver,
		retry:                retry,
		switchRepeatSleep:    switchSleep,
		closed:               false,
	}
	d.init()
	return d
}

func (this *DoubleBufferMessagePool) logf(format string, data ...interface{}) {
	if this.logRecord != nil {
		d := fmt.Sprintf("[MessagePool] %s", fmt.Sprintf(format, data...))
		this.logRecord(d)
	}
}

func (this *DoubleBufferMessagePool) SetLogRecord(logger LogRecord) {
	this.logRecord = logger
}

func (this *DoubleBufferMessagePool) PublishMessage(d interface{}) {
	this.messageChannel <- d
}

func (this *DoubleBufferMessagePool) init() {
	cPos := _POS_FLAG_0
	tPos := _POS_FLAG_1

	this.bufferPool = [2][]interface{}{
		[]interface{}{},
		[]interface{}{},
	}
	this.messageChannel = make(chan interface{}, this.messageChannelLength)
	this.posFlag = uint8(cPos)
	this.producerBufferPool = &this.bufferPool[cPos]
	this.consumerBufferPool = &this.bufferPool[tPos]
	this.producerLock = sync.Mutex{}
	this.consumerLock = sync.Mutex{}

	this.logf("init success!")

	this.run()
}

func (this *DoubleBufferMessagePool) run() {
	go func() {
		this.logf("producer start!")
		defer this.logf("producer finish!")
		this.Producer()
	}()

	go func() {
		this.logf("consumer start!")
		defer this.logf("consumer finish!")
		for {
			this.Consumer()
		}
	}()
}

func (this *DoubleBufferMessagePool) Producer() {
	for msg := range this.messageChannel {
		this.producerLock.Lock()
		*this.producerBufferPool = append(*this.producerBufferPool, msg)
		this.producerBufferVolume++
		this.logf("[producer] push success! volume=%d", this.producerBufferVolume)
		this.producerLock.Unlock()
	}
}

func (this *DoubleBufferMessagePool) Consumer() {
	this.consumerLock.Lock()
	for _, msg := range *this.consumerBufferPool {
		this.consumerBufferVolume--
		for i := 0; i <= this.retry; i++ {
			err := this.messageReceiver(msg)
			if err == nil {
				break
			}
		}
		this.logf("[consumer] receive success! volume=%d", this.consumerBufferVolume)
	}

	// clear
	*this.consumerBufferPool = (*this.consumerBufferPool)[0:0]
	this.consumerBufferVolume = 0
	this.logf("[consumer] clear success! volume=%d", this.consumerBufferVolume)

	this.consumerLock.Unlock()

	this.Switch()
}

func (this *DoubleBufferMessagePool) Switch() {
	for {
		flag := true
		this.producerLock.Lock()
		if len(*this.producerBufferPool) == 0 {
			this.logf("[switch] producer buffer is empty!")
			flag = false
		}
		this.producerLock.Unlock()

		if flag {
			this.consumerLock.Lock()
			if len(*this.consumerBufferPool) > 0 {
				this.logf("[switch] consumer buffer is not empty!")
				flag = false
			}
			this.consumerLock.Unlock()
		}

		// 如果需要切换
		if flag {
			this.consumerLock.Lock()
			this.producerLock.Lock()

			cPos := this.posFlag
			tPos := _POS_FLAG_1
			if cPos == _POS_FLAG_1 {
				tPos = _POS_FLAG_0
			}

			// 数据交换
			this.producerBufferVolume, this.consumerBufferVolume = this.consumerBufferVolume, this.producerBufferVolume
			this.producerBufferPool, this.consumerBufferPool = this.consumerBufferPool, this.producerBufferPool

			this.posFlag = uint8(tPos)

			this.logf("[switch] switch success! cPos: %d, proVolume=%d, conVolume=%d", cPos, this.producerBufferVolume, this.consumerBufferVolume)

			this.producerLock.Unlock()
			this.consumerLock.Unlock()

			break
		}

		// 不需要切换，则sleep
		this.logf("[switch] switch no need! sleep %+v", this.switchRepeatSleep)
		time.Sleep(this.switchRepeatSleep)
	}
}
