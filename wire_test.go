package qwire

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pablo-botella/quevu/qkernel"
	"github.com/pablo-botella/quevu/qpanel"
)

type rig struct {
	panel  *qkernel.Panel
	web    *pen
	client *Client
}

// pen writes lines to a screen the way a log would, but at once: these
// tests are about the wire, not about queues.
type pen struct {
	s   *qpanel.Screen
	seq uint64
}

func (p *pen) Write(b []byte) (int, error) {
	for _, line := range strings.Split(strings.TrimSuffix(string(b), "\n"), "\n") {
		p.seq++
		p.s.Write([]qkernel.Chunk{{Seq: p.seq, Time: time.Now(), Data: []byte(line)}})
	}
	return len(b), nil
}

// newRig serves a small application over an in-memory connection.
func newRig(t *testing.T) *rig {
	t.Helper()
	p := qkernel.New()
	web := &pen{s: qpanel.New("web")}
	p.AddSource(web.s)
	p.AddSource(qpanel.New("mail"))
	p.Add("web", qkernel.Func(nil, nil))
	p.Add("slow", qkernel.Go(func(ctx context.Context) error {
		<-ctx.Done()
		time.Sleep(30 * time.Millisecond) // the work in hand
		return nil
	}))
	p.Add("broken", qkernel.Func(func(context.Context) error { return errors.New("no license") }, nil))

	a, b := net.Pipe()
	go ServeConn(a, p)
	c, err := NewClient(b)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c.Close()
		p.Shutdown()
	})
	return &rig{panel: p, web: web, client: c}
}

func texts(cs []qkernel.Chunk) string {
	parts := make([]string, len(cs))
	for i, c := range cs {
		parts[i] = string(c.Data)
	}
	return strings.Join(parts, ",")
}

func recv(t *testing.T, f *qkernel.Feed) qkernel.Delivery {
	t.Helper()
	select {
	case d, ok := <-f.Data:
		if !ok {
			t.Fatal("feed closed")
		}
		return d
	case <-time.After(3 * time.Second):
		t.Fatal("no delivery")
	}
	return qkernel.Delivery{}
}

func TestUnitsAndSwitchesAcrossTheWire(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	us := r.client.Units()
	if len(us) != 3 || us[0].Name != "web" || us[0].State != qkernel.Stopped {
		t.Fatalf("units: %+v", us)
	}

	var mu sync.Mutex
	var seen []string
	cancel := r.client.Watch(func(i qkernel.UnitInfo) {
		mu.Lock()
		seen = append(seen, i.Name+":"+i.State.String())
		mu.Unlock()
	})
	defer cancel()

	if err := r.client.Start(ctx, "slow"); err != nil {
		t.Fatal(err)
	}
	// The stop returns only when the application says the unit is done.
	began := time.Now()
	if err := r.client.Stop(ctx, "slow"); err != nil {
		t.Fatal(err)
	}
	if time.Since(began) < 25*time.Millisecond {
		t.Fatal("stop returned before the work in hand was finished")
	}
	// Errors travel as text.
	err := r.client.Start(ctx, "broken")
	if err == nil || !strings.Contains(err.Error(), "no license") {
		t.Fatalf("got %v", err)
	}
	if err := r.client.Start(ctx, "nope"); err == nil {
		t.Fatal("unknown unit accepted")
	}

	want := "slow:starting slow:running slow:stopping slow:stopped broken:starting broken:failed"
	deadline := time.Now().Add(3 * time.Second)
	for {
		mu.Lock()
		got := strings.Join(seen, " ")
		mu.Unlock()
		if got == want {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("watch saw %q", got)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if us := r.client.Units(); us[2].State != qkernel.Failed || us[2].Err != "no license" {
		t.Fatalf("broken: %+v", us[2])
	}
}

func TestGlobalSwitch(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	if err := r.client.StartAll(ctx); err == nil {
		t.Fatal("the broken unit should fail the global start")
	}
	if us := r.panel.Units(); us[0].State != qkernel.Running || us[1].State != qkernel.Running {
		t.Fatalf("units: %+v", us)
	}
	if err := r.client.StopAll(ctx); err != nil {
		t.Fatal(err)
	}
	if us := r.panel.Units(); us[0].State != qkernel.Stopped || us[1].State != qkernel.Stopped {
		t.Fatalf("units: %+v", us)
	}
}

func TestStopDeadlineReachesTheApplication(t *testing.T) {
	p := qkernel.New()
	release := make(chan struct{})
	p.Add("stuck", qkernel.Go(func(ctx context.Context) error {
		<-ctx.Done()
		<-release
		return nil
	}))
	a, b := net.Pipe()
	go ServeConn(a, p)
	c, err := NewClient(b)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	defer close(release)
	c.Start(context.Background(), "stuck")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := c.Stop(ctx, "stuck"); err == nil {
		t.Fatal("stop of a stuck unit returned no error")
	}
	// The application gave up at the same deadline, and says so.
	deadline := time.Now().Add(3 * time.Second)
	for p.Units()[0].State != qkernel.Failed {
		if time.Now().After(deadline) {
			t.Fatalf("state %v", p.Units()[0].State)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestSourcesByPosition(t *testing.T) {
	r := newRig(t)
	for i := 1; i <= 20; i++ {
		fmt.Fprintf(r.web, "l%d\n", i)
	}
	srcs := r.client.Sources()
	if len(srcs) != 2 || srcs[0].Name() != "web" || srcs[1].Name() != "mail" {
		t.Fatalf("sources: %v", srcs)
	}
	web := srcs[0]
	if got, err := web.Tail(2); err != nil || texts(got) != "l19,l20" {
		t.Fatalf("tail: %q %v", texts(got), err)
	}
	if got, err := web.Before(10, 2); err != nil || texts(got) != "l8,l9" {
		t.Fatalf("before: %q %v", texts(got), err)
	}
	got, err := web.After(10, 2)
	if err != nil || texts(got) != "l11,l12" || got[0].Seq != 11 || got[0].Time.IsZero() {
		t.Fatalf("after: %q %v", texts(got), err)
	}
	if st := web.(qkernel.StatSource).Stats(); len(st) != 0 {
		t.Fatalf("stats: %+v", st)
	}
}

func TestSearchAcrossTheWire(t *testing.T) {
	r := newRig(t)
	for i := 1; i <= 300; i++ {
		if i%100 == 0 {
			fmt.Fprintf(r.web, "l%d \x1b[31mERROR\x1b[0m here\n", i)
		} else {
			fmt.Fprintf(r.web, "l%d\n", i)
		}
	}
	web := r.client.Sources()[0]
	// The step runs in the application; only the match comes back.
	res, err := qkernel.Search(web, qkernel.Query{Text: "error here"})
	if err != nil || len(res.Matches) != 1 || res.Matches[0].Seq != 300 || res.Reached != 300 {
		t.Fatalf("%+v %v", res, err)
	}
	res, _ = qkernel.Search(web, qkernel.Query{Text: "error", From: res.Reached})
	if len(res.Matches) != 1 || res.Matches[0].Seq != 200 || res.Examined != 100 || res.ReachedTime == 0 {
		t.Fatalf("second step: %+v", res)
	}
	res, _ = qkernel.Search(web, qkernel.Query{Text: "error", From: 100, Forward: true})
	if len(res.Matches) != 1 || res.Matches[0].Seq != 200 {
		t.Fatalf("forwards: %+v", res)
	}
	// A time range travels too, and so does the reason a step stopped.
	res, _ = qkernel.Search(web, qkernel.Query{Text: "error", Since: time.Now().Add(time.Hour)})
	if len(res.Matches) != 0 || !res.Done || !res.OutOfRange || res.Examined != 1 {
		t.Fatalf("out of range: %+v", res)
	}
	res, _ = qkernel.Search(web, qkernel.Query{Text: "error", Since: time.Now().Add(-time.Hour), Until: time.Now().Add(time.Hour)})
	if len(res.Matches) != 1 || res.Matches[0].Seq != 300 {
		t.Fatalf("in range: %+v", res)
	}
	res, _ = qkernel.Search(web, qkernel.Query{Text: "nothing like it", Budget: 50})
	if len(res.Matches) != 0 || res.Examined != 50 || res.Done {
		t.Fatalf("budget: %+v", res)
	}
}

// history is a provider that holds an older stretch of a log.
type history struct{ chunks []qkernel.Chunk }

func (h history) Tail(n int) ([]qkernel.Chunk, error) {
	return h.chunks[max(len(h.chunks)-n, 0):], nil
}

func (h history) Before(seq uint64, n int) ([]qkernel.Chunk, error) {
	end := 0
	for end < len(h.chunks) && h.chunks[end].Seq < seq {
		end++
	}
	return h.chunks[max(end-n, 0):end], nil
}

func (h history) After(seq uint64, n int) ([]qkernel.Chunk, error) {
	start := 0
	for start < len(h.chunks) && h.chunks[start].Seq <= seq {
		start++
	}
	return h.chunks[start:min(start+n, len(h.chunks))], nil
}

func TestTheOfferOfHistoryCrossesTheWire(t *testing.T) {
	// A log of 60 lines: the first 40 only in the history, the screen
	// remembers the last 20.
	scr := qpanel.New("old", qpanel.WithMemory(20))
	var hist history
	for i := uint64(1); i <= 60; i++ {
		c := qkernel.Chunk{Seq: i, Time: time.Now(), Data: fmt.Appendf(nil, "l%d", i)}
		if i <= 40 {
			hist.chunks = append(hist.chunks, c)
		}
		scr.Write([]qkernel.Chunk{c})
	}
	p := qkernel.New()
	p.AddSource(qkernel.Join(scr, qkernel.Roles{Historical: hist}))
	a, b := net.Pipe()
	go ServeConn(a, p)
	c, err := NewClient(b)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	src := c.Sources()[0]

	f := src.Subscribe(0)
	defer f.Close()
	f.Req <- qkernel.Request{Op: qkernel.OpFollow, N: 5}
	recv(t, f)
	f.Req <- qkernel.Request{Op: qkernel.OpPause}
	recv(t, f)
	// Recent output ends: the controller is told there is a history.
	f.Req <- qkernel.Request{Op: qkernel.OpBefore, Seq: 45, N: 10}
	d := recv(t, f)
	if d.Op != qkernel.OpBefore || texts(d.Chunks) != "l41,l42,l43,l44" || !d.History {
		t.Fatalf("offer: %v %q history=%v", d.Op, texts(d.Chunks), d.History)
	}
	// And gets it only by asking.
	f.Req <- qkernel.Request{Op: qkernel.OpHistoryBefore, Seq: 41, N: 3}
	if d := recv(t, f); d.Op != qkernel.OpHistoryBefore || texts(d.Chunks) != "l38,l39,l40" {
		t.Fatalf("history: %v %q", d.Op, texts(d.Chunks))
	}
	// The same for a search.
	res, err := qkernel.Search(src, qkernel.Query{Text: "l7"})
	if err != nil || len(res.Matches) != 0 || !res.Done || !res.History {
		t.Fatalf("search offer: %+v %v", res, err)
	}
	res, _ = qkernel.Search(src, qkernel.Query{Text: "l7", From: res.Reached, History: true})
	if len(res.Matches) != 1 || res.Matches[0].Seq != 7 {
		t.Fatalf("search in the history: %+v", res)
	}
}

func TestFeedAcrossTheWire(t *testing.T) {
	r := newRig(t)
	fmt.Fprint(r.web, "a\nb\nc\n")
	f := r.client.Sources()[0].Subscribe(0)
	defer f.Close()

	f.Req <- qkernel.Request{Op: qkernel.OpFollow, N: 2}
	if d := recv(t, f); d.Op != qkernel.OpFollow || texts(d.Chunks) != "b,c" {
		t.Fatalf("follow: %v %q", d.Op, texts(d.Chunks))
	}
	fmt.Fprint(r.web, "d\n")
	if d := recv(t, f); d.Op != qkernel.OpLive || texts(d.Chunks) != "d" {
		t.Fatalf("live: %v %q", d.Op, texts(d.Chunks))
	}
	f.Req <- qkernel.Request{Op: qkernel.OpPause}
	if d := recv(t, f); d.Op != qkernel.OpPause || d.Full {
		t.Fatalf("pause: %+v", d)
	}
	fmt.Fprint(r.web, "e\nf\n")
	f.Req <- qkernel.Request{Op: qkernel.OpBefore, Seq: 3, N: 5}
	if d := recv(t, f); d.Op != qkernel.OpBefore || texts(d.Chunks) != "a,b" {
		t.Fatalf("before: %v %q", d.Op, texts(d.Chunks))
	}
	f.Req <- qkernel.Request{Op: qkernel.OpFollow, N: 1}
	if d := recv(t, f); d.Op != qkernel.OpFollow || texts(d.Chunks) != "f" {
		t.Fatalf("resume: %v %q", d.Op, texts(d.Chunks))
	}
}

func TestSlowControllerNeverBlocksTheApplication(t *testing.T) {
	r := newRig(t)
	f := r.client.Sources()[0].Subscribe(16)
	defer f.Close()
	f.Req <- qkernel.Request{Op: qkernel.OpFollow}
	recv(t, f)

	// The console stops reading while the application talks a lot.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 20000; i++ {
			fmt.Fprintf(r.web, "l%d\n", i)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("a controller that does not read blocked the application")
	}

	// The sign shows up, then silence, then a resume that jumps to the end.
	sawSign := false
	for i := 0; i < 50 && !sawSign; i++ {
		d := recv(t, f)
		sawSign = d.Op == qkernel.OpPause && d.Full
	}
	if !sawSign {
		t.Fatal("no do-not-disturb sign")
	}
	f.Req <- qkernel.Request{Op: qkernel.OpFollow, N: 1}
	for {
		d := recv(t, f)
		if d.Op == qkernel.OpFollow {
			if texts(d.Chunks) != "l19999" {
				t.Fatalf("resume: %q", texts(d.Chunks))
			}
			break
		}
		if d.Op == qkernel.OpLive {
			t.Fatalf("live output delivered behind the sign")
		}
	}
	// Other conversations on the same connection were never held up.
	if us := r.client.Units(); len(us) != 3 {
		t.Fatalf("units: %+v", us)
	}
}

func TestUnknownSourceEndsTheFeed(t *testing.T) {
	r := newRig(t)
	ghost := &remoteSource{c: r.client, name: "ghost"}
	f := ghost.Subscribe(0)
	select {
	case _, ok := <-f.Data:
		if ok {
			t.Fatal("delivery from a source that does not exist")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("feed not ended")
	}
	if _, err := ghost.Tail(1); err == nil {
		t.Fatal("no error")
	}
}

func TestDisconnectEndsEverythingOnTheClient(t *testing.T) {
	r := newRig(t)
	f := r.client.Sources()[0].Subscribe(0)
	r.client.Close()
	select {
	case _, ok := <-f.Data:
		if ok {
			t.Fatal("delivery after close")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("feed not ended")
	}
	if err := r.client.Start(context.Background(), "web"); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v", err)
	}
	if us := r.client.Units(); us != nil {
		t.Fatalf("units: %+v", us)
	}
	select {
	case <-r.client.Done():
	default:
		t.Fatal("Done not closed")
	}
}

func TestNotQuevuAtTheOtherEnd(t *testing.T) {
	a, b := net.Pipe()
	go func() {
		a.Write([]byte("HTTP/1.1 400 Bad Request\r\n"))
		a.Close()
	}()
	if _, err := NewClient(b); err == nil {
		t.Fatal("accepted a peer that does not speak quevu")
	}
}

// The real local transport of this system: a named pipe on Windows, a
// Unix domain socket elsewhere.
func TestLocalEndpoint(t *testing.T) {
	name := fmt.Sprintf("test-%d", time.Now().UnixNano())
	p := qkernel.New()
	lg := &pen{s: qpanel.New("out")}
	p.AddSource(lg.s)
	p.Add("a", qkernel.Func(nil, nil))
	defer p.Shutdown()

	l, err := Listen(name)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- Serve(l, p) }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Two controllers at once.
	c1, err := Connect(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	c2, err := Connect(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	if err := c1.Start(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if us := c2.Units(); len(us) != 1 || us[0].State != qkernel.Running {
		t.Fatalf("second controller sees %+v", us)
	}
	fmt.Fprint(lg, "hello\n")
	if got, err := c2.Sources()[0].Tail(1); err != nil || texts(got) != "hello" {
		t.Fatalf("tail: %q %v", texts(got), err)
	}

	l.Close()
	if err := <-served; err != nil {
		t.Fatalf("serve: %v", err)
	}
	// While earlier connections live, a pipe with no listener reports
	// busy rather than missing, so a dial waits as long as it is allowed.
	short, cancelShort := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelShort()
	if _, err := Connect(short, name); err == nil {
		t.Fatal("connected to a closed endpoint")
	}
	if _, err := Listen("bad name!"); err == nil {
		t.Fatal("bad name accepted")
	}
}
