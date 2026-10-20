package main

import (
	"context"
	"expvar"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ingest/internal/api"
	"ingest/internal/dedup"
	"ingest/internal/events"
	"ingest/internal/lib"
	"ingest/internal/middleware"
	"ingest/internal/services"
	"ingest/internal/storage"
	"ingest/internal/worker"
)

func main() {
	// --- Context with graceful shutdown ---
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// --- Storage ---
	// Phase 4: swap storage.New("./uploads") for storage.NewS3(storage.S3Config{
	//     Endpoint:  lib.Getenv("S3_ENDPOINT", ""),
	//     Bucket:    lib.Getenv("S3_BUCKET", "ingest"),
	//     AccessKey: lib.Getenv("S3_ACCESS_KEY", ""),
	//     SecretKey: lib.Getenv("S3_SECRET_KEY", ""),
	//     Secure:    lib.Getenv("S3_SECURE", "") != "",
	// })
	store, err := storage.New("./uploads")
	if err != nil {
		log.Fatalf("storage: %v", err)
	}

	// --- Event queue ---
	queue := events.NewMemoryQueue(64)
	worker.RegisterThumbnail(queue)

	// Phase 6c: uncomment to enable virus scanning via ClamAV.
	// scanner := worker.NewScanner(
	//     lib.Getenv("CLAMD_ADDR", "unix:///var/run/clamav/clamd.ctl"),
	//     services.NewStore(), store,
	// )
	// scanner.Register(queue)

	go queue.Run(ctx)

	// --- Core components ---
	sessions := services.NewStore()
	dedupIdx := dedup.New()
	uploads := api.New(store, sessions, dedupIdx, queue)

	// --- Middleware ---
	rl := middleware.NewRateLimiter(20, 40) // 20 req/s, burst 40
	authToken := lib.Getenv("AUTH_TOKEN", "")

	// --- Routes ---
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", health)
	mux.Handle("GET /debug/vars", expvar.Handler())

	// Static pages — served from ./web/ relative to the working directory.
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.ServeFile(w, r, "./web/landing.html")
			return
		}
		http.NotFound(w, r)
	})
	mux.HandleFunc("GET /demo", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./web/demo.html")
	})

	mux.HandleFunc("POST /uploads", uploads.CreateUpload)
	mux.HandleFunc("PATCH /uploads/{id}", uploads.AppendChunk)
	mux.HandleFunc("HEAD /uploads/{id}", uploads.HeadUpload)
	mux.HandleFunc("PUT /uploads/{id}", uploads.WriteUpload)
	mux.HandleFunc("GET /uploads/{id}", uploads.GetUpload)

	// Middleware stack (outermost → innermost):
	//   RateLimit → BearerAuth → RequestID → mux
	var handler http.Handler = mux
	handler = middleware.RequestID(handler)
	handler = middleware.BearerAuth(authToken)(handler)
	handler = rl.Middleware(handler)

	// --- Server ---
	server := &http.Server{
		Addr:              lib.Getenv("PORT", ":8080"),
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      0, // intentionally 0: large uploads stream through here
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		slog.Info("listening", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server: %v", err)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")

	shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(shutCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}

func health(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}
