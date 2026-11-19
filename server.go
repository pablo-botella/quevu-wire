package qwire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/pablo-botella/linereader"
	"github.com/pablo-botella/quevu"
)

// Serve accepts connections on l and serves the controller on each one,
// until the listener is closed. Several controller programs may be
// connected at the same time.
func Serve(l net.Listener, c quevu.Controller) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go ServeConn(conn, c)
	}
}

// ServeConn serves the controller on one connection and returns when the
// connection ends. It closes conn.
func ServeConn(conn io.ReadWriteCloser, c quevu.Controller) error {
	ctx, cancel := context.WithCancel(context.Background())
	s := &session{
		c:      c,
		conn:   conn,
		ctx:    ctx,
		feeds:  make(map[uint64]*quevu.Feed),
		byName: make(map[string]quevu.Source),
		units:  newQueue[quevu.UnitInfo](),
	}
	for _, src := range c.Sources() {
		s.byName[src.Name()] = src
	}
	defer func() {
		cancel()
		conn.Close()
		s.units.close()
		s.mu.Lock()
		feeds := s.feeds
		s.feeds = nil
		s.mu.Unlock()
		for _, f := range feeds {
			f.Close()
		}
		s.wg.Wait()
	}()

	if err := s.send(&msg{Ev: "hello", V: Version}); err != nil {
		return err
	}
	unwatch := c.Watch(s.units.put)
	defer unwatch()
	s.wg.Add(1)
	go s.pumpUnits()

	lr := linereader.NewLineReader(conn, 64<<10, maxLine)
	for {
		line, err := lr.ReadLine()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if len(line) == 0 {
			continue
		}
		var m msg
		if err := json.Unmarshal(line, &m); err != nil {
			return fmt.Errorf("qwire: bad message: %w", err)
		}
		s.dispatch(&m)
	}
}

type session struct {
	c    quevu.Controller
	conn io.ReadWriteCloser
	ctx  context.Context
	wg   sync.WaitGroup

	wmu sync.Mutex // one writer on the connection at a time

	mu     sync.Mutex
	feeds  map[uint64]*quevu.Feed
	byName map[string]quevu.Source
	units  *queue[quevu.UnitInfo]
}

func (s *session) send(m *msg) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	s.wmu.Lock()
	defer s.wmu.Unlock()
	_, err = s.conn.Write(b)
	return err
}

func (s *session) pumpUnits() {
	defer s.wg.Done()
	for {
		info, ok := s.units.get()
		if !ok {
			return
		}
		w := toWUnit(info)
		if s.send(&msg{Ev: "unit", Info: &w}) != nil {
			return
		}
	}
}

// dispatch runs on the reading goroutine. What belongs to a feed is
// handled here, in order; everything else may take long (a stop waits for
// work in hand) and gets a goroutine of its own.
func (s *session) dispatch(m *msg) {
	switch m.Op {
	case "subscribe":
		s.subscribe(m)
	case "req":
		s.mu.Lock()
		f := s.feeds[m.Feed]
		s.mu.Unlock()
		if f == nil || m.Req == nil {
			return
		}
		r := quevu.Request{Op: quevu.Op(m.Req.Op), Seq: m.Req.Seq, N: m.Req.N}
		select {
		case f.Req <- r:
		case <-s.ctx.Done():
		}
	case "close":
		s.mu.Lock()
		f := s.feeds[m.Feed]
		delete(s.feeds, m.Feed)
		s.mu.Unlock()
		if f != nil {
			f.Close()
		}
	default:
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			re := s.handle(m)
			re.ID, re.Re = m.ID, true
			s.send(re)
		}()
	}
}

func (s *session) subscribe(m *msg) {
	src := s.byName[m.Source]
	if src == nil {
		s.send(&msg{Ev: "feedend", Feed: m.Feed, Err: fmt.Sprintf("unknown source %q", m.Source)})
		return
	}
	f := src.Subscribe(m.Buffer)
	s.mu.Lock()
	if s.feeds == nil || s.feeds[m.Feed] != nil {
		s.mu.Unlock()
		f.Close()
		s.send(&msg{Ev: "feedend", Feed: m.Feed, Err: "feed id in use"})
		return
	}
	s.feeds[m.Feed] = f
	s.mu.Unlock()

	id := m.Feed
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		// A controller that does not read slows this loop down, which
		// leaves the feed unread, which makes the log hang the do not
		// disturb sign: the log itself never waits for a connection.
		for d := range f.Data {
			w := &wDelivery{Op: int(d.Op), Chunks: toWChunks(d.Chunks), Full: d.Full, Hist: d.History, Err: errText(d.Err)}
			if s.send(&msg{Ev: "delivery", Feed: id, D: w}) != nil {
				f.Close()
				return
			}
		}
		s.send(&msg{Ev: "feedend", Feed: id})
	}()
}

func (s *session) opCtx(m *msg) (context.Context, context.CancelFunc) {
	if m.TimeoutMs > 0 {
		return context.WithTimeout(s.ctx, time.Duration(m.TimeoutMs)*time.Millisecond)
	}
	// No deadline from the caller: the operation takes what it needs,
	// and goes on even if this controller disconnects meanwhile.
	return context.WithCancel(context.Background())
}

func (s *session) handle(m *msg) *msg {
	re := &msg{}
	switch m.Op {
	case "units":
		for _, u := range s.c.Units() {
			re.Units = append(re.Units, toWUnit(u))
		}
	case "sources":
		for _, src := range s.c.Sources() {
			re.Sources = append(re.Sources, src.Name())
		}
	case "start", "stop", "startall", "stopall":
		ctx, cancel := s.opCtx(m)
		defer cancel()
		var err error
		switch m.Op {
		case "start":
			err = s.c.Start(ctx, m.Unit)
		case "stop":
			err = s.c.Stop(ctx, m.Unit)
		case "startall":
			err = s.c.StartAll(ctx)
		case "stopall":
			err = s.c.StopAll(ctx)
		}
		re.Err = errText(err)
	case "tail", "before", "after":
		src := s.byName[m.Source]
		if src == nil {
			re.Err = fmt.Sprintf("unknown source %q", m.Source)
			break
		}
		var cs []quevu.Chunk
		var err error
		switch m.Op {
		case "tail":
			cs, err = src.Tail(m.N)
		case "before":
			cs, err = src.Before(m.Seq, m.N)
		case "after":
			cs, err = src.After(m.Seq, m.N)
		}
		re.Chunks, re.Err = toWChunks(cs), errText(err)
	case "search":
		src := s.byName[m.Source]
		if src == nil {
			re.Err = fmt.Sprintf("unknown source %q", m.Source)
			break
		}
		// One bounded step. The source, and the stored-data provider
		// behind it, keep their say on how much that is.
		r, err := quevu.Search(src, quevu.Query{Text: m.Text, From: m.Seq, Forward: m.Fwd, Max: m.N, Budget: m.Budget, History: m.Hist})
		re.Chunks, re.Err = toWChunks(r.Matches), errText(err)
		re.Seq, re.RT, re.Examined, re.Done, re.Hist = r.Reached, r.ReachedTime, r.Examined, r.Done, r.History
	case "stats":
		src := s.byName[m.Source]
		if src == nil {
			re.Err = fmt.Sprintf("unknown source %q", m.Source)
			break
		}
		if ss, ok := src.(quevu.StatSource); ok {
			for _, st := range ss.Stats() {
				re.Stats = append(re.Stats, wStats{
					Queued: st.Queued, Capacity: st.Capacity, Written: st.Written,
					Dropped: st.Dropped, Failed: st.Failed, LastErr: errText(st.LastErr),
				})
			}
		}
	default:
		re.Err = fmt.Sprintf("unknown op %q", m.Op)
	}
	return re
}
