package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

const (
	serverAddress = "127.0.0.1:8080"
	timeout       = 60 * time.Second
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Welcome to the SweeetDB CLI")
	fmt.Println("Type commands like 'SET key value', 'GET key', 'DEL key' or 'exit' to quit")

	conn, err := net.DialTimeout("tcp", serverAddress, timeout)
	if err != nil {
		fmt.Println("failed to connect to the server: %w", err)
	}
	defer conn.Close()

	for {
		fmt.Print("> ")
		scanner.Scan()
		input := scanner.Text()
		command := strings.TrimSpace(input)

		if strings.ToLower(command) == "connect" {
			conn, _ = net.Dial("tcp", serverAddress)
			command = "PING"
		}

		if strings.ToLower(command) == "exit" {
			break
		}

		response, err := sendCommand(command, conn)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Println(response)
		}
	}
}

func sendCommand(command string, conn net.Conn) (string, error) {
	startTime := time.Now() // capture start time

	err := conn.SetWriteDeadline(time.Now().Add(timeout))
	if err != nil {
		return "", fmt.Errorf("failed to set write deadline: %w", err)
	}

	_, err = conn.Write([]byte(command))
	if err != nil {
		return "", fmt.Errorf("failed to send command: %w", err)
	}

	err = conn.SetReadDeadline(time.Now().Add(timeout))
	if err != nil {
		return "", fmt.Errorf("failed to set read deadline: %w", err)
	}

	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	elapsedTime := time.Since(startTime)
	fmt.Printf("Time: %v\n", elapsedTime)

	return strings.TrimSpace(response), nil
}
