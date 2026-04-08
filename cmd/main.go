package main

import (
	"fmt"
	"net/http"

	"bitcask"
)

var bc *bitcask.Bitcask

func main() {
	var err error
	bc, err = bitcask.NewInitBitcask()
	if err != nil {
		fmt.Println("Error initializing Bitcask:", err)
		return
	}
	fmt.Println("Bitcask is running!")
	defer bc.Close()

	http.HandleFunc("/set", setHandler)
	http.HandleFunc("/get", getHandler)
	http.HandleFunc("/delete", deleteHandler)

	fmt.Println("Starting HTTP server on :8080")
	http.ListenAndServe(":8080", nil)
}

func setHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", 405)
		return
	}
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")
	if key == "" || value == "" {
		http.Error(w, "Missing key or value", 400)
		return
	}
	err := bc.Set(key, []byte(value))
	if err != nil {
		http.Error(w, "Error setting value", 500)
		return
	}
	w.WriteHeader(200)
	w.Write([]byte("OK"))
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", 405)
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key", 400)
		return
	}
	val, err := bc.Get(key)
	if err != nil {
		http.Error(w, "Error getting value", 500)
		return
	}
	if val == nil {
		http.Error(w, "Key not found", 404)
		return
	}
	w.WriteHeader(200)
	w.Write(val)
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "DELETE" {
		http.Error(w, "Method not allowed", 405)
		return
	}
	// Placeholder - delete not implemented in Bitcask yet
	http.Error(w, "Delete not implemented", 501)
}
