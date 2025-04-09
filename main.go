package main

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func main() {
	var LIMIT_KEY = 5
	// Initialize Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // Change if needed
		Password: "",               // No password by default
		DB:       0,                // Default DB
		PoolSize: 10,               //Connection pools
	})
	var cursor uint64 = 0
	matchPattern := "subscription-callback:*" // Pattern to match keys
	count := int64(100)                       // Limit to 100 keys per scan

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
