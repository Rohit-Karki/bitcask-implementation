package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bitcask"
	"bitcask/web"
)

func main() {
	bc, err := bitcask.NewInitBitcask()
	if err != nil {
		fmt.Println("Error initializing Bitcask:", err)
		return
	}
	fmt.Println("Bitcask is running!")

	web.InitHandlers(bc)

	server := &http.Server{
		Addr:    ":8080",
		Handler: nil, // uses default mux
	}

	// Channel to listen for interrupt signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	http.HandleFunc("/set", web.SetHandler)
	http.HandleFunc("/get", web.GetHandler)
	http.HandleFunc("/delete", web.DeleteHandler)

	go func() {
		fmt.Println("Starting HTTP server on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("HTTP server error: %v\n", err)
		}
	}()

	// Wait for interrupt signal
	<-c
	fmt.Println("\nShutting down server...")

	// Graceful shutdown with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		fmt.Printf("Server shutdown error: %v\n", err)
	}

	// Close Bitcask after server shutdown
	bc.Close()
	fmt.Println("Bitcask closed.")
}
