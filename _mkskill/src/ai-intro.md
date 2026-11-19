---
mkskill:
  pos: 5
  in: ai*
---

# quevu-wire - agent notes

Go package `github.com/pablo-botella/quevu-wire`, imported as `qwire`: a
`quevu.Controller` served over a stream. Server side: `Serve`,
`ServeConn`. Client side: `Client` (implements `quevu.Controller`; its
sources implement `quevu.Source`, `quevu.Searcher` and `quevu.StatSource`). Transport:
`Listen`, `Dial`, `Connect`, `Endpoint`. Depends on `quevu`,
`github.com/pablo-botella/linereader` and, on Windows,
`github.com/Microsoft/go-winio`. Tests also use `quevu-panel`.
