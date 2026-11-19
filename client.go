package qwire

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/pablo-botella/linereader"
	"github.com/pablo-botella/quevu"
)

// Client is the far end of a served panel. It implements
// quevu.Controller, so a visual part uses it exactly as it would use the
// panel of its own process.
type Client struct {
	conn io.ReadWriteCloser
	wmu  sync.Mutex

	mu       sync.Mutex
	nextID   uint64
	pending  map[uint64]chan *msg
	feeds    map[uint64]*clientFeed
	watchers map[uint64]func(quevu.UnitInfo)
	sources  []quevu.Source

	units  *queue[quevu.UnitInfo]
	closed chan struct{}
	once   sync.Once
	err    error
}

// NewClient starts a client over an established connection and checks
// the hello of the server.
func NewClient(conn io.ReadWriteCloser) (*Client, error) {
	c := &Client{
		conn:     conn,
		pending:  make(map[uint64]chan *msg),
		feeds:    make(map[uint64]*clientFeed),
		watchers: make(map[uint64]func(quevu.UnitInfo)),
		units:    newQueue[quevu.UnitInfo](),
		closed:   make(chan struct{}),
	}
	lr := linereader.NewLineReader(conn, 64<<10, maxLine)
	line, err := lr.ReadLine()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("qwire: no hello: %w", err)
	}
	var hello msg
	if err := json.Unmarshal(line, &hello); err != nil || hello.Ev != "hello" {
		conn.Close()
		return nil, fmt.Errorf("qwire: the peer does not speak quevu")
	}
	if hello.V != Version {
		conn.Close()
		return nil, fmt.Errorf("qwire: protocol version %d, this client speaks %d", hello.V, Version)
	}
	go c.read(lr)
	go c.pumpUnits()
	return c, nil
}

// Close ends the connection. Feeds end and pending calls return
// ErrClosed.
func (c *Client) Close() error {
	c.shutdown(ErrClosed)
	return nil
}

// Done is closed when the connection has ended, whoever ended it.
func (c *Client) Done() <-chan struct{} { return c.closed }

func (c *Client) shutdown(err error) {
	c.once.Do(func() {
		c.mu.Lock()
		c.err = err
		feeds := c.feeds
		c.feeds = make(map[uint64]*clientFeed)
		c.mu.Unlock()
		close(c.closed)
		c.conn.Close()
		c.units.close()
		for _, f := range feeds {
			f.end()
		}
	})
}

func (c *Client) send(m *msg) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, err = c.conn.Write(b)
	return err
}

// read is the only reader of the connection. It never waits for anybody:
// answers go to a buffered channel, everything else to a mailbox.
func (c *Client) read(lr *linereader.LineReader) {
	for {
		line, err := lr.ReadLine()
		if err != nil {
			if err == io.EOF {
				err = ErrClosed
			}
			c.shutdown(err)
			return
		}
		if len(line) == 0 {
			continue
		}
		var m msg
		if err := json.Unmarshal(line, &m); err != nil {
			c.shutdown(fmt.Errorf("qwire: bad message: %w", err))
			return
		}
		switch {
		case m.Re:
			c.mu.Lock()
			ch := c.pending[m.ID]
			delete(c.pending, m.ID)
			c.mu.Unlock()
			if ch != nil {
				ch <- &m
			}
		case m.Ev == "unit" && m.Info != nil:
			c.units.put(fromWUnit(*m.Info))
		case m.Ev == "delivery" && m.D != nil:
			c.mu.Lock()
			f := c.feeds[m.Feed]
			c.mu.Unlock()
			if f != nil {
				f.deliver(quevu.Delivery{
					Op:     quevu.Op(m.D.Op),
					Chunks: fromWChunks(m.D.Chunks),
					Full:   m.D.Full,
					Err:    textErr(m.D.Err),
				})
			}
		case m.Ev == "feedend":
			c.mu.Lock()
			f := c.feeds[m.Feed]
			delete(c.feeds, m.Feed)
			c.mu.Unlock()
			if f != nil {
				f.end()
			}
		}
	}
}

func (c *Client) pumpUnits() {
	for {
		info, ok := c.units.get()
		if !ok {
			return
		}
		c.mu.Lock()
		fns := make([]func(quevu.UnitInfo), 0, len(c.watchers))
		for _, fn := range c.watchers {
			fns = append(fns, fn)
		}
		c.mu.Unlock()
		for _, fn := range fns {
			fn(info)
		}
	}
}

func (c *Client) call(ctx context.Context, m *msg) (*msg, error) {
	ch := make(chan *msg, 1)
	c.mu.Lock()
	if c.err != nil {
		err := c.err
		c.mu.Unlock()
		return nil, err
	}
	c.nextID++
	m.ID = c.nextID
	c.pending[m.ID] = ch
	c.mu.Unlock()
	drop := func() {
		c.mu.Lock()
		delete(c.pending, m.ID)
		c.mu.Unlock()
	}
	if err := c.send(m); err != nil {
		drop()
		return nil, err
	}
	select {
	case re := <-ch:
		return re, textErr(re.Err)
	case <-ctx.Done():
		drop()
		return nil, ctx.Err()
	case <-c.closed:
		drop()
		c.mu.Lock()
		defer c.mu.Unlock()
		return nil, c.err
	}
}

// Units lists the units of the application. On a broken connection it
// returns nothing.
func (c *Client) Units() []quevu.UnitInfo {
	re, err := c.call(context.Background(), &msg{Op: "units"})
	if err != nil {
		return nil
	}
	out := make([]quevu.UnitInfo, len(re.Units))
	for i, u := range re.Units {
		out[i] = fromWUnit(u)
	}
	return out
}

func (c *Client) control(ctx context.Context, op, unit string) error {
	m := &msg{Op: op, Unit: unit}
	if dl, ok := ctx.Deadline(); ok {
		// The application gets the same patience the caller has.
		m.TimeoutMs = max(time.Until(dl).Milliseconds(), 1)
	}
	_, err := c.call(ctx, m)
	return err
}

// Start starts one unit of the application.
func (c *Client) Start(ctx context.Context, name string) error {
	return c.control(ctx, "start", name)
}

// Stop stops one unit of the application in an orderly way. If ctx ends
// first the call returns, and the application goes on stopping the unit.
func (c *Client) Stop(ctx context.Context, name string) error {
	return c.control(ctx, "stop", name)
}

// StartAll is the global switch, on.
func (c *Client) StartAll(ctx context.Context) error { return c.control(ctx, "startall", "") }

// StopAll is the global switch, off.
func (c *Client) StopAll(ctx context.Context) error { return c.control(ctx, "stopall", "") }

// Watch calls fn on every change of state of a unit. fn runs on a
// goroutine of the client, one call at a time.
func (c *Client) Watch(fn func(quevu.UnitInfo)) (cancel func()) {
	c.mu.Lock()
	c.nextID++
	id := c.nextID
	c.watchers[id] = fn
	c.mu.Unlock()
	return func() {
		c.mu.Lock()
		delete(c.watchers, id)
		c.mu.Unlock()
	}
}

// Sources lists the logs of the application.
func (c *Client) Sources() []quevu.Source {
	c.mu.Lock()
	cached := c.sources
	c.mu.Unlock()
	if cached != nil {
		return append([]quevu.Source(nil), cached...)
	}
	re, err := c.call(context.Background(), &msg{Op: "sources"})
	if err != nil {
		return nil
	}
	out := make([]quevu.Source, len(re.Sources))
	for i, name := range re.Sources {
		out[i] = &remoteSource{c: c, name: name}
	}
	c.mu.Lock()
	c.sources = out
	c.mu.Unlock()
	return append([]quevu.Source(nil), out...)
}

// remoteSource is a log seen through the connection.
type remoteSource struct {
	c    *Client
	name string
}

func (s *remoteSource) Name() string { return s.name }

func (s *remoteSource) ask(op string, seq uint64, n int) ([]quevu.Chunk, error) {
	re, err := s.c.call(context.Background(), &msg{Op: op, Source: s.name, Seq: seq, N: n})
	if err != nil {
		return nil, err
	}
	return fromWChunks(re.Chunks), nil
}

func (s *remoteSource) Tail(n int) ([]quevu.Chunk, error) { return s.ask("tail", 0, n) }

func (s *remoteSource) Before(seq uint64, n int) ([]quevu.Chunk, error) {
	return s.ask("before", seq, n)
}

func (s *remoteSource) After(seq uint64, n int) ([]quevu.Chunk, error) {
	return s.ask("after", seq, n)
}

// Search runs one bounded step of a search in the application, next to
// the data: only the matches travel.
func (s *remoteSource) Search(q quevu.Query) (quevu.Result, error) {
	re, err := s.c.call(context.Background(), &msg{
		Op: "search", Source: s.name, Text: q.Text, Seq: q.From, Fwd: q.Forward, N: q.Max, Budget: q.Budget,
	})
	if err != nil {
		return quevu.Result{Reached: q.From}, err
	}
	return quevu.Result{
		Matches: fromWChunks(re.Chunks), Reached: re.Seq, ReachedTime: re.RT, Examined: re.Examined, Done: re.Done,
	}, nil
}

// Stats shows the device queues of the log, as the application sees them.
func (s *remoteSource) Stats() []quevu.DeviceStats {
	re, err := s.c.call(context.Background(), &msg{Op: "stats", Source: s.name})
	if err != nil {
		return nil
	}
	out := make([]quevu.DeviceStats, len(re.Stats))
	for i, w := range re.Stats {
		out[i] = quevu.DeviceStats{
			Queued: w.Queued, Capacity: w.Capacity, Written: w.Written,
			Dropped: w.Dropped, Failed: w.Failed, LastErr: textErr(w.LastErr),
		}
	}
	return out
}

func (s *remoteSource) Subscribe(buffer int) *quevu.Feed {
	if buffer <= 0 {
		buffer = 1024
	}
	c := s.c
	f := &clientFeed{
		c:     c,
		limit: buffer,
		data:  make(chan quevu.Delivery),
		req:   make(chan quevu.Request, 16),
		done:  make(chan struct{}),
	}
	f.cond = sync.NewCond(&f.mu)
	c.mu.Lock()
	dead := c.err != nil
	if !dead {
		c.nextID++
		f.id = c.nextID
		c.feeds[f.id] = f
	}
	c.mu.Unlock()
	go f.pumpData()
	if dead {
		f.end()
	} else {
		c.send(&msg{Op: "subscribe", Feed: f.id, Source: s.name, Buffer: buffer})
		go f.pumpReq()
	}
	return quevu.NewFeed(f.data, f.req, func() {
		c.mu.Lock()
		delete(c.feeds, f.id)
		c.mu.Unlock()
		c.send(&msg{Op: "close", Feed: f.id})
		f.end()
	})
}

// clientFeed is the controller's end of a feed. Deliveries wait in an
// inbox so the goroutine reading the connection never waits for a
// console. The inbox obeys the same rule as the log's own buffer: when
// more live chunks are waiting than the console asked room for, it hangs
// the do not disturb sign itself and tells the application to stop
// sending.
type clientFeed struct {
	c     *Client
	id    uint64
	limit int
	data  chan quevu.Delivery
	req   chan quevu.Request
	done  chan struct{}
	once  sync.Once

	mu    sync.Mutex
	cond  *sync.Cond
	inbox []quevu.Delivery
	live  int
	dnd   bool
	ended bool
}

func (f *clientFeed) end() {
	f.once.Do(func() {
		close(f.done)
		f.mu.Lock()
		f.ended = true
		f.cond.Broadcast()
		f.mu.Unlock()
	})
}

func (f *clientFeed) deliver(d quevu.Delivery) {
	pause := false
	f.mu.Lock()
	switch {
	case d.Op == quevu.OpLive && f.dnd:
		// Sent before the application heard about the sign.
	case d.Op == quevu.OpLive:
		f.live += len(d.Chunks)
		if f.live > f.limit {
			kept := f.inbox[:0]
			for _, x := range f.inbox {
				if x.Op != quevu.OpLive {
					kept = append(kept, x)
				}
			}
			f.inbox = append(kept, quevu.Delivery{Op: quevu.OpPause, Full: true})
			f.live = 0
			f.dnd = true
			pause = true
		} else {
			f.inbox = append(f.inbox, d)
		}
	default:
		if d.Op == quevu.OpFollow || (d.Op == quevu.OpPause && d.Full) {
			f.dnd = d.Op == quevu.OpPause
		}
		f.inbox = append(f.inbox, d)
	}
	f.cond.Signal()
	f.mu.Unlock()
	if pause {
		// Not from the reading goroutine: writing may have to wait.
		go f.c.send(&msg{Op: "req", Feed: f.id, Req: &wReq{Op: int(quevu.OpPause)}})
	}
}

func (f *clientFeed) pumpData() {
	defer close(f.data)
	for {
		f.mu.Lock()
		for len(f.inbox) == 0 && !f.ended {
			f.cond.Wait()
		}
		if f.ended {
			f.mu.Unlock()
			return
		}
		d := f.inbox[0]
		f.inbox[0] = quevu.Delivery{}
		f.inbox = f.inbox[1:]
		if d.Op == quevu.OpLive {
			f.live -= len(d.Chunks)
		}
		f.mu.Unlock()
		select {
		case f.data <- d:
		case <-f.done:
			return
		}
	}
}

func (f *clientFeed) pumpReq() {
	for {
		select {
		case r := <-f.req:
			if f.c.send(&msg{Op: "req", Feed: f.id, Req: &wReq{Op: int(r.Op), Seq: r.Seq, N: r.N}}) != nil {
				return
			}
		case <-f.done:
			return
		}
	}
}

var (
	_ quevu.Controller = (*Client)(nil)
	_ quevu.Source     = (*remoteSource)(nil)
	_ quevu.StatSource = (*remoteSource)(nil)
	_ quevu.Searcher   = (*remoteSource)(nil)
)
