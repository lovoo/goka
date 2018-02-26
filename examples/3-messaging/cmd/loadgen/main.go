package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/lovoo/goka/examples/3-messaging"
)

const (
	urlTmpl  = "http://localhost:8080/%s/send"
	spamProb = 0.3
)

var (
	users = []string{
		"Alice",
		"Bob",
		"Charlie",
		"Dave",
		"Eve",
	}

	contents = []string{
		"Hi how are you doing",
		"Hello let's have lunch together",
		"Bye",
	}
)

func send(from, to, content string) {
	m := messaging.Message{
		To:      to,
		Content: content,
	}

	b, err := json.Marshal(&m)
	if err != nil {
		log.Printf("error encoding message: %v", err)
		return
	}

	url := fmt.Sprintf(urlTmpl, from)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(b))
	if err != nil {
		log.Printf("error creating request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("error sending request: %v", err)
		return
	}
	defer resp.Body.Close()
	//TODO(diogo) check response status code
}

func main() {
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()

	for range t.C {
		var (
			cnt  = "spam!"
			from = "Bob"
		)
		if rand.Float64() < 1-spamProb {
			from = users[rand.Intn(len(users))]
			cnt = contents[rand.Intn(len(contents))]
		}
		to := users[rand.Intn(len(users))]
		for to == from {
			to = users[rand.Intn(len(users))]
		}
		send(from, to, cnt)
	}
}
