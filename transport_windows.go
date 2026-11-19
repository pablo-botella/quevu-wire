package qwire

import (
	"context"
	"net"

	"github.com/Microsoft/go-winio"
)

// Endpoint returns the path of the named pipe behind a name.
func Endpoint(name string) string { return `\\.\pipe\quevu-` + name }

func listen(name string) (net.Listener, error) {
	// The default security descriptor of a pipe grants access to its
	// creator, administrators and the system.
	return winio.ListenPipe(Endpoint(name), nil)
}

func dial(ctx context.Context, name string) (net.Conn, error) {
	return winio.DialPipeContext(ctx, Endpoint(name))
}
