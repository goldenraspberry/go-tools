package message_pool

import (
	"testing"
	"time"
)

type TMessage struct {
	T    *testing.T
	Data interface{}
}

func ReceiverMessage(d interface{}) error {
	if m, ok := d.(*TMessage); ok {
		m.T.Logf("Receiver Message !!!! data=%+v", m.Data)
	}
	return nil
}

func TestDoubleBufferPush(t *testing.T) {
	m := NewDoubleBufferMessagePool(ReceiverMessage, func(msg string) {
		t.Log(msg)
	}, 10, 3, time.Duration(1000)*time.Millisecond)

	for i := 0; i <= 100; i++ {
		d := &TMessage{
			T:    t,
			Data: i,
		}
		m.PublishMessage(d)
	}

	time.Sleep(10 * time.Second)
}
