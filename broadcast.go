package phylax

import (
	"sync"
	"sync/atomic"
)

// subscription is one subscriber's delivery channel and its drop counter.
type subscription struct {
	ch chan *Change
	// dropped counts how many changes were dropped for this subscriber
	// because its buffer was full. Updated under mu by Publish; read by
	// ChangesDropped, also under mu, but atomic so either is safe.
	dropped atomic.Int64
}

type Broadcaster struct {
	mu          sync.Mutex
	subscribers map[string]*subscription
}

func NewBroadcaster() *Broadcaster {
	return &Broadcaster{
		subscribers: map[string]*subscription{},
	}
}

func (b *Broadcaster) Subscribe(id string, bufferSize int) chan *Change {
	ch := make(chan *Change, bufferSize)
	b.mu.Lock()
	b.subscribers[id] = &subscription{ch: ch}
	b.mu.Unlock()
	return ch
}

func (b *Broadcaster) Unsubscribe(id string) {
	b.mu.Lock()
	if sub, ok := b.subscribers[id]; ok {
		close(sub.ch)
		delete(b.subscribers, id)
	}
	b.mu.Unlock()

}
func (b *Broadcaster) Publish(change *Change) {
	b.mu.Lock()
	for _, sub := range b.subscribers {
		select {
		case sub.ch <- change:
		default:
			// drop the change if the subscriber is not ready to receive it,
			// and count it so the metrics stream can report it
			sub.dropped.Add(1)
		}
	}
	b.mu.Unlock()
}

// SubscriberCount returns the number of active subscribers.
func (b *Broadcaster) SubscriberCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subscribers)
}

// ChangesDropped returns the total number of changes dropped for active
// subscribers because their buffers were full.
func (b *Broadcaster) ChangesDropped() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	var total int64
	for _, sub := range b.subscribers {
		total += sub.dropped.Load()
	}
	return total
}
