package raft

import (
	"reflect"
	"sync"
	"testing"
)

func TestEffectQueuePreservesOrder(t *testing.T) {
	var mu sync.Mutex
	var received []string

	q, err := NewEffectQueue(
		16,
		func(effects Effects) error {
			mu.Lock()
			defer mu.Unlock()

			for _, message := range effects.Messages {
				received = append(
					received,
					message.To,
				)
			}

			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, peer := range []string{
		"node2",
		"node3",
		"node4",
	} {
		err := q.Enqueue(Effects{
			Messages: []OutboundMessage{
				{To: peer},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()

	expected := []string{
		"node2",
		"node3",
		"node4",
	}

	if !reflect.DeepEqual(received, expected) {
		t.Fatalf(
			"unexpected delivery order: got %v, want %v",
			received,
			expected,
		)
	}
}
