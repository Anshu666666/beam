//go:build !windows

package main

func initTerminal() {
	// POSIX terminals natively support ANSI escape codes without special initialization.
}

