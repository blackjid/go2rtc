package app

import "sync"

var (
	shutdownMu    sync.Mutex
	shutdownHooks []func()
)

// OnShutdown registers fn to run once when go2rtc exits cleanly: on a signal
// or through api/exit. Modules use it to tell remote peers they are leaving
// rather than letting them time out.
func OnShutdown(fn func()) {
	shutdownMu.Lock()
	shutdownHooks = append(shutdownHooks, fn)
	shutdownMu.Unlock()
}

// Shutdown runs the registered hooks, each at most once across calls.
func Shutdown() {
	shutdownMu.Lock()
	hooks := shutdownHooks
	shutdownHooks = nil
	shutdownMu.Unlock()

	for _, fn := range hooks {
		fn()
	}
}
