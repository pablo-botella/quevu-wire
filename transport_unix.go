//go:build !windows

package qwire

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"syscall"
)

// Endpoint returns the path of the Unix domain socket behind a name. It
// lives in the runtime directory of the user when there is one, and in
// the temporary directory otherwise.
func Endpoint(name string) string {
	dir := os.Getenv("XDG_RUNTIME_DIR")
	if dir == "" {
		dir = os.TempDir()
	}
	return filepath.Join(dir, "quevu-"+name+".sock")
}

func listen(name string) (net.Listener, error) {
	path := Endpoint(name)
	l, err := net.Listen("unix", path)
	if errors.Is(err, syscall.EADDRINUSE) {
		// A socket file outlives a process that died. If nobody answers
		// it is a leftover and can be replaced.
		if c, derr := net.Dial("unix", path); derr == nil {
			c.Close()
			return nil, err
		}
		os.Remove(path)
		l, err = net.Listen("unix", path)
	}
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		l.Close()
		return nil, err
	}
	return l, nil
}

func dial(ctx context.Context, name string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "unix", Endpoint(name))
}
