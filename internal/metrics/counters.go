package metrics

import "expvar"

// Counters are published at /debug/vars by the expvar HTTP handler.
// Register the handler in main: mux.Handle("GET /debug/vars", expvar.Handler()).
var (
	UploadsStarted      = expvar.NewInt("uploads_started")
	UploadsCompleted    = expvar.NewInt("uploads_completed")
	UploadsDeduplicated = expvar.NewInt("uploads_deduplicated")
	UploadsFlagged      = expvar.NewInt("uploads_flagged")
)
