// broadcast_test.go — tests for the fan-out Broadcaster in broadcast.go.
//
// The tests fabricate Change values and, in one case, inject a hand-made
// subscriber map directly, so they exercise only the fan-out logic and not
// any decoding or connection code.

package phylax

import (
	"testing"
	"time"
)

// fakeChange returns a fabricated Change so tests never depend on real WAL
// decoding.
func fakeChange() *Change {
	return &Change{
		Table:     "users",
		Operation: "insert",
		NewRow:    map[string]any{"id": 1, "name": "Grace", "email": "grace@example.com"},
	}
}

// receiveOrTimeout drains one Change from ch, failing the test if nothing
// arrives within a second.
func receiveOrTimeout(t *testing.T, id string, ch <-chan *Change) *Change {
	t.Helper()
	select {
	case got := <-ch:
		return got
	case <-time.After(time.Second):
		t.Fatalf("subscriber %q never received the published change", id)
		return nil
	}
}

// TestPublishFansOut verifies that one Publish delivers the same Change
// instance to every subscriber.
func TestPublishFansOut(t *testing.T) {
	b := NewBroadcaster()
	chA := b.Subscribe("a", 1)
	chB := b.Subscribe("b", 1)
	chC := b.Subscribe("c", 1)

	want := fakeChange()
	b.Publish(want)

	for id, ch := range map[string]<-chan *Change{"a": chA, "b": chB, "c": chC} {
		if got := receiveOrTimeout(t, id, ch); got != want {
			t.Errorf("subscriber %q received a different Change than the one published", id)
		}
	}
}

// TestPublishUsesSubscriberMap builds a Broadcaster with a hand-made
// subscriber map of fake channels and verifies Publish fans out over it —
// the same path the real Subscribe calls use.
func TestPublishUsesSubscriberMap(t *testing.T) {
	b := NewBroadcaster()
	fakeA := make(chan *Change, 1)
	fakeB := make(chan *Change, 1)
	b.subscribers = map[string]*subscription{
		"fake-a": {ch: fakeA},
		"fake-b": {ch: fakeB},
	}

	want := fakeChange()
	b.Publish(want)

	for id, ch := range map[string]chan *Change{"fake-a": fakeA, "fake-b": fakeB} {
		if got := receiveOrTimeout(t, id, ch); got != want {
			t.Errorf("subscriber %q received a different Change than the one published", id)
		}
	}
}

// TestUnsubscribeClosesChannel verifies Unsubscribe removes the subscriber
// from the map and closes its channel, and is a no-op for unknown ids.
func TestUnsubscribeClosesChannel(t *testing.T) {
	b := NewBroadcaster()
	ch := b.Subscribe("s", 1)

	b.Unsubscribe("s")

	if _, ok := b.subscribers["s"]; ok {
		t.Error("subscriber still present in the map after Unsubscribe")
	}
	if _, ok := <-ch; ok {
		t.Error("subscriber channel was not closed after Unsubscribe")
	}

	b.Unsubscribe("never-subscribed") // must not panic
	b.Unsubscribe("s")                // double unsubscribe must not panic
}

// TestPublishDropsWhenBufferFull verifies the non-blocking fan-out: when a
// subscriber's buffer is full, the overflowing change is dropped for that
// subscriber, and nobody blocks.
func TestPublishDropsWhenBufferFull(t *testing.T) {
	b := NewBroadcaster()
	slow := b.Subscribe("slow", 1)
	fast := b.Subscribe("fast", 1)

	first := fakeChange()
	b.Publish(first) // fills both buffers (size 1)

	// Drain both so the next publish has room again.
	if got := receiveOrTimeout(t, "slow", slow); got != first {
		t.Fatalf("slow subscriber received the wrong first change")
	}
	if got := receiveOrTimeout(t, "fast", fast); got != first {
		t.Fatalf("fast subscriber received the wrong first change")
	}

	second := fakeChange()
	b.Publish(second)       // delivered to both
	b.Publish(fakeChange()) // both buffers full → dropped for both

	// Both subscribers must hold exactly the second change and nothing
	// more — the overflowing publish was dropped.
	for id, ch := range map[string]<-chan *Change{"slow": slow, "fast": fast} {
		if got := receiveOrTimeout(t, id, ch); got != second {
			t.Errorf("subscriber %q received the wrong change", id)
		}
		select {
		case got := <-ch:
			t.Errorf("drop failed: subscriber %q received an extra change: %v", id, got)
		default:
			// expected: the overflowing change was dropped
		}
	}
}

// TestConcurrentFanOut hammers Subscribe/Publish/Unsubscribe from a
// goroutine while the main goroutine publishes, so the mutex-protected map
// is exercised concurrently (run with -race).
func TestConcurrentFanOut(t *testing.T) {
	b := NewBroadcaster()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			b.Subscribe("s", 2)
			b.Publish(fakeChange())
			b.Unsubscribe("s")
		}
	}()

	for i := 0; i < 100; i++ {
		b.Publish(fakeChange())
	}
	<-done
}
