// Package local installs Heimdall's optional llama.cpp generator backend.
// Hosts import this package; plugins import pkg/heimdall alone so their shared
// package ABI does not depend on whether the host includes local inference.
package local
