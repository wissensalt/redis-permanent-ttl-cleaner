package main

import (
	"bufio"
	"context"
	"github.com/redis/go-redis/v9"
	"io"
	"log"
	"os"
	"strings"
	"time"
	"unicode"
)

const (
	HOST      = "localhost"
	PORT      = "6379"
	USERNAME  = ""
	PASSWORD  = "password"
	DELIMITER = "###"
	FileName  = "./backup.txt"
)

var ctx = context.Background()

func main() {
	printMenu()
	scanner := bufio.NewScanner(os.Stdin)
	client := connect()
	var myRedisKeyVal = make(map[string]string, 0)
	for scanner.Scan() {
		if scanner.Text() == "1" {
			ping(client)
		}

		if scanner.Text() == "2" {
			myRedisKeyVal = scanKeyAndValueWithoutTTL(client, "*")
			backupKeysWithoutTTL(myRedisKeyVal, true, false)
		}

		if scanner.Text() == "3" {
			myRedisKeyVal = scanKeyAndValueWithoutTTL(client, "*")
			backupKeysWithoutTTL(myRedisKeyVal, false, false)
		}

		if scanner.Text() == "4" {
			myRedisKeyVal = readInput(FileName)
			setTTLForNoExpiryKeys(client, myRedisKeyVal)
		}

		if scanner.Text() == "5" {
			myRedisKeyVal = readInput(FileName)
			deleteKeys(client, myRedisKeyVal)
		}

		if scanner.Text() == "6" {
			myRedisKeyVal = readInput(FileName)
			restoreKeyValues(client, myRedisKeyVal)
		}

		if scanner.Text() == "7" {
			deleteKeysViaIterator(client)
		}

		if scanner.Text() == "0" {
			os.Exit(1)
		}

		printMenu()
	}
}

func printMenu() {
	log.Println()
	log.Println("### REDIS PERMANENT TTL CLEANER ###")
	log.Println("1: PING")
	log.Println("2: BACKUP FOR KEYS WITHOUT TTL")
	log.Println("3: BACKUP FOR KEYS WITHOUT TTL (NO FILTER)")
	log.Println("4: SET VALID TTL FOR KEYS WITHOUT TTL")
	log.Println("5: DELETE FOR KEYS WITHOUT TTL")
	log.Println("6: RESTORE KEYS")
	log.Println("7: DELETE FOR KEYS WITHOUT TTL VIA ITERATOR")
	log.Println("0: EXIT")
	log.Println()
}

func connect() *redis.Client {

	return redis.NewClient(&redis.Options{
		Addr:     HOST + ":" + PORT,
		Username: USERNAME,
		Password: PASSWORD,
		DB:       0,
	})
}

func backupKeysWithoutTTL(myRedisKeyVal map[string]string, filterAsciiPrintable bool, includeValue bool) {
	if _, err := os.Stat(FileName); err == nil {
		os.Remove(FileName)
	}

	f, err := os.OpenFile(FileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}

	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			log.Fatal("Failed to close file")
		}
	}(f)

	for key, val := range myRedisKeyVal {
		if filterAsciiPrintable {
			if isAsciiPrintable(val) {
				writeFile(f, key, val, includeValue)
			}
		} else {
			writeFile(f, key, val, includeValue)
		}
	}
	log.Printf("COMPLETED BACKUP KEYS WITHOUT TTL. TOTAL: %v\n", len(myRedisKeyVal))
}

func writeFile(f *os.File, key string, val string, includeValue bool) {
	if includeValue {
		if _, err := f.WriteString(key + DELIMITER + val + "\n"); err != nil {
			log.Println(err)
		}
	} else {
		if _, err := f.WriteString(key + "\n"); err != nil {
			log.Println(err)
		}
	}
}

func scanKeyAndValueWithoutTTL(client *redis.Client, key string) map[string]string {
	var cursor uint64
	var result = make(map[string]string, 0)
	for {
		var keys []string
		var err error
		keys, cursor, err = client.Scan(ctx, cursor, key, 0).Result()
		if err != nil {
			panic(err)
		}

		for _, key := range keys {
			log.Printf("SCANNING KEY: %v\n", key)
			ttl, err := client.TTL(ctx, key).Result()
			if err != nil {
				continue
			}

			if ttl == -1 {
				log.Printf("KEY %v HAS NO TTL: ", key)
				val, err := client.Get(ctx, key).Result()
				if err != nil {
					continue
				}

				result[key] = val
			}
		}

		if cursor == 0 { // no more keys
			log.Printf("COMPLETED SCAN KEYS WITHOUT TTL TOTAL: %v\n", len(result))

			break
		}
	}

	return result
}

func ping(client *redis.Client) {
	pong, err := client.Ping(ctx).Result()
	log.Println(pong, err)
}

// For Use case key with unicode character. Example: �
func deleteKeysViaIterator(client *redis.Client) {
	var cursor uint64
	scanIterator := client.Scan(ctx, cursor, "*", 0).Iterator()
	counter := 0
	for scanIterator.Next(ctx) {
		key := scanIterator.Val()
		ttl, err := client.TTL(ctx, key).Result()
		if err != nil {
			continue
		}

		if ttl == -1 {
			log.Printf("DELETING KEY :%v\n", key)
			if err := client.Unlink(ctx, key).Err(); err != nil {
				log.Fatalf("FAILED TO DELETE KEY %v\n", key)
			}
			counter++
		}

		if counter > 0 && counter%1000 == 0 {
			sleepDurationInSeconds := 10
			log.Printf("Sleeping in %v Seconds for cooling down Redis\n", sleepDurationInSeconds)
			time.Sleep(time.Duration(sleepDurationInSeconds) * time.Second)
		}
	}

	log.Printf("Completed delete Permanent Redis Keys via Iterator with Total Keys %v\n", counter)
}

func deleteKeys(client *redis.Client, keys map[string]string) {
	for key, _ := range keys {
		log.Println("DELETING KEY: ", key)
		if err := client.Unlink(ctx, key).Err(); err != nil {
			log.Fatalf("FAILED TO DELETE KEY %v", key)
		}
	}

	log.Printf("COMPLETED DELETE %v KEYS\n", len(keys))
}

func readInput(path string) map[string]string {
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("Error opening file: %v\n", err)
		os.Exit(1)
	}

	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalf("Error closing file: %v\n", err)
		}
	}(file)
	reader := bufio.NewReader(file)
	var myRedisKeyVal = make(map[string]string, 0)
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}
		input := string(line)
		if line != nil && input != "" {
			separatedInput := strings.Split(input, DELIMITER)
			if len(separatedInput) == 2 && !strings.Contains(input, "otp") {
				myRedisKeyVal[separatedInput[0]] = separatedInput[1]
			}

		}
	}

	return myRedisKeyVal
}

func restoreKeyValues(client *redis.Client, myRedisKeyValues map[string]string) {
	for key, value := range myRedisKeyValues {
		err := client.Set(ctx, key, value, -1).Err()
		if err != nil {
			log.Printf("FAILED TO RESTORE KEY: %v\n", key)
		}
	}

	log.Printf("COMPLETED RESTORE KEYS TOTAL: %v\n", len(myRedisKeyValues))
}

func setTTLForNoExpiryKeys(client *redis.Client, myRedisKeyValues map[string]string) {
	for key, _ := range myRedisKeyValues {
		_, err := client.Expire(ctx, key, 1*time.Hour).Result()
		if err != nil {
			log.Printf("FAILED TO UPDATE TTL FOR KEY %v\n", key)
		} else {
			log.Printf("SUCCESS TO UPDATE TTL FOR KEY %v\n", key)
		}
	}

	log.Printf("COMPLETED UPDATE TTL KEYS TOTAL: %v\n", len(myRedisKeyValues))
}

func isAsciiPrintable(s string) bool {
	for _, r := range s {
		if r > unicode.MaxASCII || !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}
