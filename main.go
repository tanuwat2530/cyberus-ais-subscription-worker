package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()
var LIMIT_KEY = 10
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
	fmt.Println("##### START WORKER PROCESS #####")
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // Change if needed
		Password: "",               // No password by default
		DB:       0,                // Default DB
		PoolSize: 10,               //Connection pools
	})

	var wg sync.WaitGroup

	for range ticker.C {
		// Perform the scan with the match pattern and count
		keys, newCursor, err := rdb.Scan(ctx, cursor, matchPattern, count).Result()

		if err != nil {
			panic(err)
		}
		for { // for infinity loop

			// Process only the first key found (if any)
			if len(keys) >= LIMIT_KEY {
				for i := 0; i < LIMIT_KEY; i++ {
					//fmt.Println("Send Key to Worker : ", keys[i]) // Print only the first key
					// Example: Get the value of the key (assuming it's a string)
					valJson, err := rdb.Get(ctx, keys[i]).Result()
					if err != nil {
						fmt.Println("Error getting value:", err)
					} else {
						//fmt.Println("Value:", val)
						// Start multiple goroutines (threads)
						wg.Add(1)
						go worker(i, &wg, valJson)
						rdb.Del(ctx, keys[i]).Result()
						// Wait for all goroutines to finish
					}
				}
			}

			if len(keys) > 0 && len(keys) < LIMIT_KEY {
				//fmt.Println("Send Key to Worker : ", keys[i]) // Print only the first key
				// Example: Get the value of the key (assuming it's a string)
				valJson, err := rdb.Get(ctx, keys[0]).Result()
				if err != nil {
					fmt.Println("Error getting value:", err)
				} else {
					//fmt.Println("Value:", val)
					// Start multiple goroutines (threads)
					wg.Add(1)
					go worker(0, &wg, valJson)
					rdb.Del(ctx, keys[0]).Result()
					// Wait for all goroutines to finish
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

// Function that simulates work for a thread
func worker(id int, wg *sync.WaitGroup, data string) {
	defer wg.Done() // Mark this goroutine as done when it exits
	fmt.Printf("Worker No : %d\n", id)
	fmt.Println("Process data : " + data)
	time.Sleep(10000 * time.Millisecond) // Simulate work
	fmt.Printf("Worker No : %d finished\n", id)
}
