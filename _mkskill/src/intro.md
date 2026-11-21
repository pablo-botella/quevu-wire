---
mkskill:
  pos: 10
  in: readme
---

# quevu-wire

A [quevu](https://github.com/pablo-botella/quevu) panel across a local
connection, so the window can run in a process of its own: the
application serves its panel, and a controller program gets back
something that behaves exactly like it.

In the application, typically a service with no window:

```go
ln, _ := qwire.Listen("billing")
go qwire.Serve(ln, panel)
```

In the controller:

```go
client, _ := qwire.Connect(ctx, "billing")
defer client.Close()

client.Stop(ctx, "mailer")           // returns when the mailer says it is done
tail, _ := client.Sources()[0].Tail(200)
```

`client` is a `quevu.Controller`, the same interface `*quevu.Panel`
implements. Whatever shows a panel shows a remote one without knowing.
