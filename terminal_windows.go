//go:build windows

package main

import (
	"os"
	"syscall"
	"unsafe"
)

func initTerminal() {
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	procGetConsoleMode := kernel32.NewProc("GetConsoleMode")
	procSetConsoleMode := kernel32.NewProc("SetConsoleMode")

	stdout := syscall.Handle(os.Stdout.Fd())
	var mode uint32

	r1, _, _ := procGetConsoleMode.Call(uintptr(stdout), uintptr(unsafe.Pointer(&mode)))
	if r1 != 0 {
		const enableVTP = 0x0004
		procSetConsoleMode.Call(uintptr(stdout), uintptr(mode|enableVTP))
	}
}

