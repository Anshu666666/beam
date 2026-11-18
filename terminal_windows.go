//go:build windows

package main

import (
	"os"
	"syscall"
	"unsafe"
)

// initTerminal enables Windows ANSI escape (Virtual Terminal Processing)
// support on both stdout and stderr so colored output renders on older
// conhost consoles. POSIX terminals support ANSI natively.
func initTerminal() {
	enableVTP(syscall.Handle(os.Stdout.Fd()))
	enableVTP(syscall.Handle(os.Stderr.Fd()))
}

func enableVTP(handle syscall.Handle) {
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	procGetConsoleMode := kernel32.NewProc("GetConsoleMode")
	procSetConsoleMode := kernel32.NewProc("SetConsoleMode")

	var mode uint32
	r1, _, _ := procGetConsoleMode.Call(uintptr(handle), uintptr(unsafe.Pointer(&mode)))
	if r1 != 0 {
		const enableVTP = 0x0004
		procSetConsoleMode.Call(uintptr(handle), uintptr(mode|enableVTP))
	}
}
