package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"LsmStorageEngine/engine"
	"LsmStorageEngine/types"
)

func main() {
	se := engine.CreateNewEngine()
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("LSM Storage Engine CLI")
	fmt.Println("Commands: get <key>, put <key> <value>, delete <key>, exit")

	for {
		fmt.Print("> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			continue
		}
		input = strings.TrimSpace(input)
		if input == "exit" {
			break
		}
		args := strings.Fields(input)
		if len(args) == 0 {
			continue
		}
		cmd := args[0]
		switch cmd {
		case "get":
			if len(args) != 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			key := []byte(args[1])
			ch := se.Get(key)
			res := <-ch
			if res.Err != nil {
				fmt.Println("Error:", res.Err)
			} else if res.Record.Key == nil {
				fmt.Println("Key not found")
			} else {
				fmt.Printf("Key: %s, Value: %s\n", string(res.Record.Key), string(res.Record.Value))
			}
		case "put":
			if len(args) != 3 {
				fmt.Println("Usage: put <key> <value>")
				continue
			}
			key := []byte(args[1])
			value := []byte(args[2])
			record := types.Record{Key: key, Value: value}
			ch := se.Put(record)
			res := <-ch
			if res.Err != nil {
				fmt.Println("Error:", res.Err)
			} else {
				fmt.Printf("Put Key: %s, Value: %s\n", args[1], args[2])
			}
		case "delete":
			if len(args) != 2 {
				fmt.Println("Usage: delete <key>")
				continue
			}
			key := []byte(args[1])
			ch := se.Delete(key)
			res := <-ch
			if res.Err != nil {
				fmt.Println("Error:", res.Err)
			} else {
				fmt.Printf("Deleted Key: %s\n", args[1])
			}
		default:
			fmt.Println("Unknown command")
		}
	}
	fmt.Println("Exiting CLI.")
}
