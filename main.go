package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"encoding/json"

	"github.com/redis/go-redis/v9"
)

func main() {
	// Start background worker in a goroutine
	go backgroundWorker()
	// Keep main running so the program doesn't exit
	select {} // This blocks forever
}

// This is the background job that runs forever
func backgroundWorker() {

	var ctx = context.Background()
	var LIMIT_KEY = 10
	var cursor uint64 = 0
	var matchPattern = "subscription-callback:*" // Pattern to match keys
	var count = int64(100)                       // Limit to 100 keys per scan

	fmt.Println("##### BACKGROUUND PROCESS RUNNING #####")
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379", // Change if needed
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
						go threadWorker(i, &wg, valJson)
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
					go threadWorker(0, &wg, valJson)
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
func threadWorker(id int, wg *sync.WaitGroup, jsonString string) {
	type DataRequest struct {
		ClientIP    string `json:"ClientIP"`
		ProviderUrl string `json:"ProviderUrl"`
		Action      string `json:"action"`
		Code        string `json:"code"`
		Desc        string `json:"desc"`
		Media       string `json:"media"`
		Msisdn      string `json:"msisdn"`
		Operator    string `json:"operator"`
		Refid       string `json:"refid"`
		Shortcode   string `json:"shortcode"`
		Timestamp   string `json:"timestamp"`
		Token       string `json:"token"`
		Tranref     string `json:"tranref"`
		ClientID    string `json:"ClientID"`
		RedisKey    string `Json:"RedisKey"`
	}

	var ctx = context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379", // Change if needed
		Password: "",               // No password by default
		DB:       0,                // Default DB
		PoolSize: 10,               //Connection pools
	})

	defer wg.Done() // Mark this goroutine as done when it exits
	fmt.Printf("Worker No : %d\n", id)

	// Convert struct to JSON string
	var dataRequest DataRequest
	err := json.Unmarshal([]byte(jsonString), &dataRequest)
	if err != nil {
		fmt.Println("JSON Marshal error:", err)
	}
	fmt.Println("sending request : " + dataRequest.RedisKey)

	redis_key := "send-request:" + dataRequest.RedisKey
	ttl := 240 * time.Hour // expires in 10 day

	// Set key with TTL
	err = rdb.Set(ctx, redis_key, "return", ttl).Err()
	if err != nil {
		fmt.Println("Redis SET error:", err)

	}

	fmt.Printf("Worker No : %d SET REDIS finished\n", id)
}
