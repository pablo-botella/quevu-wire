// Package qwire carries a quevu panel across a local connection, so the
// visual part can run in a process of its own: the application serves its
// quevu.Controller, and the controller program gets back a
// quevu.Controller that behaves like the original.
//
// # Transport
//
// Listen and Dial use what each system offers for local conversation:
// named pipes on Windows, Unix domain sockets everywhere else. Both are
// local to the machine and reachable only by the user that created them.
// Serve and NewClient take any stream, so another transport can be
// plugged in without touching the protocol.
//
// # Protocol
//
// One JSON object per line, in both directions. It is not tied to Go: a
// controller written in another language only needs a JSON library.
//
// The server opens with a hello carrying the protocol version. After
// that the client sends requests, each with an id the answer repeats, and
// the server sends answers and events:
//
//	-> {"id":1,"op":"units"}
//	<- {"id":1,"re":true,"units":[{"name":"web","state":2}]}
//	-> {"id":2,"op":"stop","unit":"web","timeout_ms":30000}
//	<- {"ev":"unit","info":{"name":"web","state":3}}
//	<- {"ev":"unit","info":{"name":"web","state":0}}
//	<- {"id":2,"re":true}
//
// A feed keeps its two one-way lanes. The client names it, sends its
// requests up without waiting for anything, and the deliveries come down
// as events in the order the log produced them:
//
//	-> {"op":"subscribe","feed":7,"source":"web","buffer":1024}
//	-> {"op":"req","feed":7,"req":{"op":2,"n":200}}
//	<- {"ev":"delivery","feed":7,"d":{"op":2,"chunks":[...]}}
//	<- {"ev":"delivery","feed":7,"d":{"op":1,"chunks":[...]}}
//	-> {"op":"close","feed":7}
//
// Chunk data travels as base64, since output is not always text.
package qwire

import (
	"errors"
	"time"

	quevu "github.com/pablo-botella/quevu-kernel"
)

// Version is the protocol version announced in the hello.
const Version = 1

// maxLine bounds one protocol line. A delivery of a few hundred chunks is
// far below it; it is a guard against a peer that is not speaking quevu.
const maxLine = 64 << 20

// ErrClosed is returned by a client whose connection has ended.
var ErrClosed = errors.New("qwire: connection closed")

type msg struct {
	ID uint64 `json:"id,omitempty"`
	Op string `json:"op,omitempty"`
	Ev string `json:"ev,omitempty"`
	Re bool   `json:"re,omitempty"`

	V         int        `json:"v,omitempty"`
	Unit      string     `json:"unit,omitempty"`
	Source    string     `json:"source,omitempty"`
	Feed      uint64     `json:"feed,omitempty"`
	Seq       uint64     `json:"seq,omitempty"`
	N         int        `json:"n,omitempty"`
	Buffer    int        `json:"buffer,omitempty"`
	TimeoutMs int64      `json:"timeout_ms,omitempty"`
	Req       *wReq      `json:"req,omitempty"`
	D         *wDelivery `json:"d,omitempty"`
	Info      *wUnit     `json:"info,omitempty"`
	Units     []wUnit    `json:"units,omitempty"`
	Sources   []string   `json:"sources,omitempty"`
	Chunks    []wChunk   `json:"chunks,omitempty"`
	Stats     []wStats   `json:"stats,omitempty"`
	Text      string     `json:"text,omitempty"`
	Fwd       bool       `json:"fwd,omitempty"`
	Budget    int        `json:"budget,omitempty"`
	Examined  int        `json:"examined,omitempty"`
	RT        int64      `json:"rt,omitempty"`
	Done      bool       `json:"done,omitempty"`
	Hist      bool       `json:"hist,omitempty"`
	Since     int64      `json:"since,omitempty"`
	Until     int64      `json:"until,omitempty"`
	OOR       bool       `json:"oor,omitempty"`
	Err       string     `json:"err,omitempty"`
}

type wReq struct {
	Op  int    `json:"op"`
	Seq uint64 `json:"seq,omitempty"`
	N   int    `json:"n,omitempty"`
}

type wDelivery struct {
	Op     int      `json:"op"`
	Chunks []wChunk `json:"chunks,omitempty"`
	Full   bool     `json:"full,omitempty"`
	Hist   bool     `json:"hist,omitempty"`
	Err    string   `json:"err,omitempty"`
}

type wChunk struct {
	S uint64 `json:"s"`
	T int64  `json:"t"`
	D []byte `json:"d"`
}

type wUnit struct {
	Name  string `json:"name"`
	State int    `json:"state"`
	Err   string `json:"err,omitempty"`
}

type wStats struct {
	Name     string `json:"name,omitempty"`
	Queued   int    `json:"queued"`
	Capacity int    `json:"capacity"`
	Written  uint64 `json:"written"`
	Skipped  uint64 `json:"skipped"`
	Waiting  bool   `json:"waiting,omitempty"`
	Waits    uint64 `json:"waits,omitempty"`
	Failed   uint64 `json:"failed"`
	LastErr  string `json:"last_err,omitempty"`
}

func toWChunks(cs []quevu.Chunk) []wChunk {
	if len(cs) == 0 {
		return nil
	}
	out := make([]wChunk, len(cs))
	for i, c := range cs {
		d := c.Data
		if d == nil {
			d = []byte{}
		}
		out[i] = wChunk{S: c.Seq, T: c.Time.UnixNano(), D: d}
	}
	return out
}

func fromWChunks(ws []wChunk) []quevu.Chunk {
	if len(ws) == 0 {
		return nil
	}
	out := make([]quevu.Chunk, len(ws))
	for i, w := range ws {
		out[i] = quevu.Chunk{Seq: w.S, Time: time.Unix(0, w.T), Data: w.D}
	}
	return out
}

func toWUnit(i quevu.UnitInfo) wUnit {
	return wUnit{Name: i.Name, State: int(i.State), Err: i.Err}
}

func fromWUnit(w wUnit) quevu.UnitInfo {
	return quevu.UnitInfo{Name: w.Name, State: quevu.State(w.State), Err: w.Err}
}

// wTime and wNano carry a time as nanoseconds since the Unix epoch, with
// zero for "no time".
func wTime(ns int64) time.Time {
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

func wNano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func textErr(s string) error {
	if s == "" {
		return nil
	}
	return errors.New(s)
}
