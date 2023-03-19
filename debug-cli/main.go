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
	timeout       = 5 * time.Second
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Welcome to the SweeetDB CLI")
	fmt.Println("Type commands like 'SET key value', 'GET key', 'DEL key' or 'exit' to quit")

	for {
		fmt.Print("> ")
		scanner.Scan()
		input := scanner.Text()
		command := strings.TrimSpace(input)

		if strings.ToLower(command) == "exit" {
			break
		}

		response, err := sendCommand(command)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Println(response)
		}
	}
}

func sendCommand(command string) (string, error) {
	conn, err := net.DialTimeout("tcp", serverAddress, timeout)
	if err != nil {
		return "", fmt.Errorf("failed to connect to the server: %w", err)
	}
	defer conn.Close()

	err = conn.SetWriteDeadline(time.Now().Add(timeout))
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

	return strings.TrimSpace(response), nil
}
