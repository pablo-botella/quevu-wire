package qwire

import (
	"context"
	"fmt"
	"net"
	"regexp"
)

var nameRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

func checkName(name string) error {
	if !nameRE.MatchString(name) {
		return fmt.Errorf("qwire: invalid endpoint name %q (letters, digits, dot, dash and underscore, up to 64)", name)
	}
	return nil
}

// Listen opens the local endpoint an application is reached at. name
// identifies the application on this machine: on Windows it becomes a
// named pipe, elsewhere a Unix domain socket. Only the user that created
// it can connect.
func Listen(name string) (net.Listener, error) {
	if err := checkName(name); err != nil {
		return nil, err
	}
	return listen(name)
}

// Dial connects to the endpoint of an application on this machine.
func Dial(ctx context.Context, name string) (net.Conn, error) {
	if err := checkName(name); err != nil {
		return nil, err
	}
	return dial(ctx, name)
}

// Connect dials the endpoint of an application and returns its panel.
func Connect(ctx context.Context, name string) (*Client, error) {
	conn, err := Dial(ctx, name)
	if err != nil {
		return nil, err
	}
	return NewClient(conn)
}
