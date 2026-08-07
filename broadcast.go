package phylax

import "sync"

type Broadcaster struct {
	mu          sync.Mutex
	subscribers map[string]chan *Change
}

func NewBroadcaster() *Broadcaster {
	return &Broadcaster{
		subscribers: map[string]chan *Change{},
	}
}

func (b *Broadcaster) Subscribe(id string, bufferSize int) chan *Change {
	ch := make(chan *Change, bufferSize)
	b.mu.Lock()
	b.subscribers[id] = ch
	b.mu.Unlock()
	return ch
}

func (b *Broadcaster) Unsubscribe(id string) {
	b.mu.Lock()
	if ch, ok := b.subscribers[id]; ok {
		close(ch)
		delete(b.subscribers, id)
	}
	b.mu.Unlock()

}
func (b *Broadcaster) Publish(change *Change) {
	b.mu.Lock()
	for _, ch := range b.subscribers {
		select {
		case ch <- change:
		default:
			// drop the change if the subscriber is not ready to receive it
		}
	}
	b.mu.Unlock()
}
