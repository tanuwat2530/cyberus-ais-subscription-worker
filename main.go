package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

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
	var matchPattern = "subscription-callback-api:*" // Pattern to match keys
	var count = int64(100)                           // Limit to 100 keys per scan

	fmt.Println("##### BACKGROUUND PROCESS RUNNING #####")

	rdb := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379", // Change if needed
		Password: "",               // No password by default
		DB:       0,                // Default DB
		PoolSize: 10,               //Connection pools
	})

	var wg sync.WaitGroup

	for {

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
	type TransactionData struct {
		Msisdn       string `json:"msisdn"`
		Shortcode    string `json:"short-code"`
		Operator     string `json:"operator"`
		Action       string `json:"action"`
		Code         string `json:"code"`
		Desc         string `json:"desc"`
		Timestamp    int    `json:"timestamp"`
		TranRef      string `json:"tran-ref"`
		RefId        string `json:"ref-id"`
		Media        string `json:"media"`
		Token        string `json:"token"`
		ReturnStatus string `json:"cuberus-return"`
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
	var transactionData TransactionData
	err := json.Unmarshal([]byte(jsonString), &transactionData)
	if err != nil {
		fmt.Println("JSON Marshal error:", err)
	}

	// // Print the data to the console
	// fmt.Println("##### Insert into Database #####")
	// fmt.Println("Msisdn : " + transactionData.Msisdn)
	// fmt.Println("Shortcode : " + transactionData.Shortcode)
	// fmt.Println("Operator  : " + transactionData.Operator)
	// fmt.Println("Action  : " + transactionData.Action)
	// fmt.Println("Code  : " + transactionData.Code)
	// fmt.Println("Desc  : " + transactionData.Desc)
	//fmt.Println("Timestamp  : " + strconv.FormatInt(int64(transactionData.Timestamp)))
	// fmt.Println("TranRef  : " + transactionData.TranRef)
	// fmt.Println("Action  : " + transactionData.Action)
	// fmt.Println("RefId  : " + transactionData.RefId)
	// fmt.Println("Media  : " + transactionData.Media)
	// fmt.Println("Token  : " + transactionData.Token)
	// fmt.Println("CyberusReturn  : " + transactionData.ReturnStatus)

	redis_key := "transaction-log-worker:" + transactionData.Media + ":" + transactionData.RefId
	ttl := 240 * time.Hour // expires in 10 day
	// Set key with TTL
	err = rdb.Set(ctx, redis_key, jsonString, ttl).Err()
	if err != nil {
		fmt.Println("Redis SET error:", err)

	}

	fmt.Printf("Worker No : %d SET REDIS finished\n", id)
}
