# quevu-wire - agent notes

Go package `github.com/pablo-botella/quevu-wire`, imported as `qwire`: a
`quevu.Controller` served over a stream. Server side: `Serve`,
`ServeConn`. Client side: `Client` (implements `quevu.Controller`; its
sources implement `quevu.Source`, `quevu.Searcher` and `quevu.StatSource`). Transport:
`Listen`, `Dial`, `Connect`, `Endpoint`. Depends on `quevu`,
`github.com/pablo-botella/linereader` and, on Windows,
`github.com/Microsoft/go-winio`.

## Transport

`Listen(name)` and `Dial(ctx, name)` use what each system offers for local
conversation:

| System | Endpoint |
|---|---|
| Windows | Named pipe `\\.\pipe\quevu-<name>` |
| Others | Unix domain socket `quevu-<name>.sock` in `$XDG_RUNTIME_DIR`, or the temporary directory |

Both are local to the machine and reachable only by the user that created
them. `Endpoint(name)` returns the path. A name is letters, digits, dot,
dash and underscore, up to 64.

`Serve` and `NewClient` take any `net.Listener` and any
`io.ReadWriteCloser`, so another transport (TCP behind a tunnel, for
instance) plugs in without touching the protocol. Authentication is then
the business of that transport: the protocol has none.

Several controllers can be connected to one application at once.

## Protocol

One JSON object per line, in both directions. It is not tied to Go: a
controller in another language only needs a JSON library.

The server opens with a hello carrying the protocol version. Then the
client sends requests, each with an id the answer repeats, and the server
sends answers and events:

```
-> {"id":1,"op":"units"}
<- {"id":1,"re":true,"units":[{"name":"web","state":2}]}
-> {"id":2,"op":"stop","unit":"web","timeout_ms":30000}
<- {"ev":"unit","info":{"name":"web","state":3}}
<- {"ev":"unit","info":{"name":"web","state":0}}
<- {"id":2,"re":true}
```

Requests: `units`, `sources`, `start`, `stop`, `startall`, `stopall`,
`tail`, `before`, `after`, `search`, `stats`. Unit states: 0 stopped, 1 starting,
2 running, 3 stopping, 4 failed.

A feed keeps its two one-way lanes. The client names it, sends its
requests up without waiting for anything, and the deliveries come down as
events in the order the log produced them:

```
-> {"op":"subscribe","feed":7,"source":"web","buffer":1024}
-> {"op":"req","feed":7,"req":{"op":2,"n":200}}
<- {"ev":"delivery","feed":7,"d":{"op":2,"chunks":[{"s":41,"t":1767322245000000000,"d":"R0VUIC8="}]}}
<- {"ev":"delivery","feed":7,"d":{"op":1,"chunks":[...]}}
-> {"op":"close","feed":7}
<- {"ev":"feedend","feed":7}
```

Feed operations: 1 live, 2 follow, 3 pause, 4 tail, 5 before, 6 after,
7 history-before, 8 history-after. A delivery that answers `before` with
`hist` set says recent output ran short and a history exists; the two
history operations are sent only after the user asked for it. A
chunk is `s` (position), `t` (nanoseconds since the Unix epoch) and `d`
(data, base64, since output is not always text).

A search is one bounded step, run in the application, next to the data;
only the matches travel:

```
-> {"id":9,"op":"search","source":"web","text":"timeout","seq":0,"budget":100000}
<- {"id":9,"re":true,"chunks":[...],"seq":48211,"rt":1767322245000000000,"examined":51790}
```

`seq` in the request is where the step starts (zero, going backwards, is
the end of the log) and `fwd` walks towards newer output. In the answer,
`seq` and `rt` are the position and time the step reached, `examined` how
many chunks it looked at, and `done` says it ran out of log rather than
out of budget. `hist` in the request allows the step into the history
(the user asked); `hist` in the answer offers it. `since` and `until`,
nanoseconds since the Unix epoch, bound the search in time; `oor` in the
answer says the step stopped because it walked past that range.

## Behaviour worth knowing

- **A stop takes what it takes.** The call returns when the application
  says the unit has stopped. If the caller's context has a deadline it
  travels as `timeout_ms` and the application gives up at the same moment;
  with no deadline the stop goes on even if the controller disconnects.
- **A controller that does not read never blocks the application.** The
  goroutine that carries a feed slows down, the feed goes unread, and the
  log hangs the *do not disturb* sign, exactly as with a local console.
  The client applies the same rule to its own inbox.
- **Errors travel as text.** `errors.Is` does not work across the wire.
- **A disconnect ends everything on the client:** feeds close, pending
  calls return `ErrClosed`, `Done()` is closed.

## Rules and gotchas

- **`Units()` and `Sources()` return nothing on a broken connection**:
  the interface they implement has no error to return. Check `Done()`.
- **`Watch` callbacks run one at a time** on a goroutine of the client and
  may call back into it.
- **On Windows a pipe with live connections but no listener reports busy,
  not missing**, so a dial waits as long as its context allows.
- **Internals for changes:** the reading goroutine on each side must never
  wait. On the client, answers go to buffered channels and everything else
  to mailboxes; on the server, requests that belong to a feed are handled
  in order on the reading goroutine and every other request gets a
  goroutine, because a stop may take long.
