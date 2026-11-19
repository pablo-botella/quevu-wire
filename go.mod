module github.com/pablo-botella/quevu-wire

go 1.26.0

require github.com/pablo-botella/quevu v0.0.0

require (
	github.com/Microsoft/go-winio v0.6.4
	github.com/pablo-botella/linereader v0.5.2
	github.com/pablo-botella/quevu-log v0.0.0
	golang.org/x/sys v0.49.0 // indirect
)

replace github.com/pablo-botella/quevu => ../quevu

replace github.com/pablo-botella/quevu-log => ../quevu-log
