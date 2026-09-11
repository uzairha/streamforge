package source

// seenSet is a bounded, FIFO-eviction set of recently observed keys. It backs
// event dedup for sources that can redeliver the same occurrence — a
// resubscribe after reconnect, or a chain reorg re-announcing a block hash
// that was already seen under the same event ID.
//
// Not safe for concurrent use; callers serialize access (the Ethereum source
// touches it only from its single reader goroutine).
type seenSet struct {
	limit int
	index map[string]struct{}
	order []string // ring of keys in insertion order, len <= limit
	next  int      // write cursor into order once it reaches limit
}

func newSeenSet(limit int) *seenSet {
	if limit <= 0 {
		limit = 1
	}
	return &seenSet{
		limit: limit,
		index: make(map[string]struct{}, limit),
		order: make([]string, 0, limit),
	}
}

// seen reports whether key was already recorded, then records it. A false
// result means this is the first time key has been observed (within the
// retention window).
func (s *seenSet) seen(key string) bool {
	if _, ok := s.index[key]; ok {
		return true
	}
	if len(s.order) < s.limit {
		s.order = append(s.order, key)
	} else {
		evict := s.order[s.next]
		delete(s.index, evict)
		s.order[s.next] = key
		s.next = (s.next + 1) % s.limit
	}
	s.index[key] = struct{}{}
	return false
}
