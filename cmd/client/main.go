package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/jainyk/distkv/internal/network"
)

func main() {
	serverAddr := flag.String("server", "localhost:50051", "Server address")
	flag.Parse()

	client, err := network.NewClient(*serverAddr)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	fmt.Println("Connected to server at", *serverAddr)
	fmt.Println("Enter commands (get <key>, put <key> <value>, delete <key>, exit)")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		line := scanner.Text()
		if line == "exit" {
			break
		}

		parts := strings.Split(line, " ")
		if len(parts) == 0 {
			continue
		}

		switch parts[0] {
		case "get":
			if len(parts) != 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			value, err := client.Get(parts[1])
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			fmt.Printf("%s\n", value)

		case "put":
			if len(parts) < 3 {
				fmt.Println("Usage: put <key> <value>")
				continue
			}
			value := strings.Join(parts[2:], " ")
			if err := client.Put(parts[1], value); err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			fmt.Println("OK")

		case "delete":
			if len(parts) != 2 {
				fmt.Println("Usage: delete <key>")
				continue
			}
			if err := client.Delete(parts[1]); err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			fmt.Println("OK")

		default:
			fmt.Println("Unknown command. Use get, put, delete, or exit")
		}
	}
}
