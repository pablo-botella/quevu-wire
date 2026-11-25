---
mkskill:
  pos: 5
  in: ai*
---

# quevu-wire - agent notes

Go package `github.com/pablo-botella/quevu-wire`, imported as `qwire`: a
`qkernel.Controller` served over a stream. Server side: `Serve`,
`ServeConn`. Client side: `Client` (implements `qkernel.Controller`; its
sources implement `qkernel.Source`, `qkernel.Searcher` and `qkernel.StatSource`). Transport:
`Listen`, `Dial`, `Connect`, `Endpoint`. Depends on `quevu`,
`github.com/pablo-botella/linereader` and, on Windows,
`github.com/Microsoft/go-winio`.
