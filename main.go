package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()
var LIMIT_KEY = 5
var cursor uint64 = 0
var matchPattern = "subscription-callback:*" // Pattern to match keys
var count = int64(100)                       // Limit to 100 keys per scan

func main() {
	// Start background worker in a goroutine
	go backgroundWorker()

	// Keep main running so the program doesn't exit
	select {} // This blocks forever
}

// This is the background job that runs forever
func backgroundWorker() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // Change if needed
		Password: "",               // No password by default
		DB:       0,                // Default DB
		PoolSize: 10,               //Connection pools
	})

	for t := range ticker.C {
		fmt.Println("Background job running at:", t.Format("15:04:05"))
		// Initialize Redis client

		for {
			// Perform the scan with the match pattern and count
			keys, newCursor, err := rdb.Scan(ctx, cursor, matchPattern, count).Result()
			if err != nil {
				panic(err)
			}
			// Process only the first key found (if any)
			if len(keys) >= LIMIT_KEY {
				for i := 0; i < LIMIT_KEY; i++ {
					fmt.Println("Send Key to Worker : ", keys[i]) // Print only the first key

					// Example: Get the value of the key (assuming it's a string)
					val, err := rdb.Get(ctx, keys[i]).Result()
					if err != nil {
						fmt.Println("Error getting value:", err)
					} else {
						fmt.Println("Value:", val)
					}

				}

			}

			// Update cursor for the next iteration
			cursor = newCursor

			// If the cursor is 0, then the scan is complete
			if cursor == 0 {
				break
			}

		}

	}
}
