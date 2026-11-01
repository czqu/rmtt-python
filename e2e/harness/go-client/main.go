// Command client is the Go side of the rmtt-python cross-language E2E matrix.
//
// Usage: client <url> <credential>
//
// Runs sequential, concurrent, binary and empty-payload round trips against an
// echo server, then disconnects. Prints CLIENT_E2E_PASS on success.
package main

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/czqu/rmtt-go/client"
)

const count = 5

func main() {
	var positional []string
	alpn := ""
	args := os.Args[1:]
	for i := 0; i < len(args); i++ {
		if args[i] == "--alpn" && i+1 < len(args) {
			alpn = args[i+1]
			i++
			continue
		}
		positional = append(positional, args[i])
	}
	if len(positional) < 2 {
		fmt.Fprintln(os.Stderr, "usage: client <url> <credential> [binarySize] [--alpn PROTO]")
		os.Exit(2)
	}
	url := positional[0]
	credential := positional[1]
	binarySize := 256 * 1024
	if len(positional) > 2 {
		if v, err := strconv.Atoi(positional[2]); err == nil && v >= 0 {
			binarySize = v
		}
	}

	opts := client.NewClientOptions()
	opts.AddServer(url)
	opts.SetCredential(credential)
	opts.SetConnectTimeout(10 * time.Second)
	opts.SetHeartbeat(10 * time.Second)
	opts.AutoReconnect = false
	opts.ConnectRetry = false
	if len(url) >= 5 && (url[:5] == "tls:/" || url[:5] == "wss:/" || url[:5] == "quic:") {
		tlsCfg := &tls.Config{InsecureSkipVerify: true}
		if url[:5] == "quic:" && alpn != "" {
			// Servers that negotiate ALPN (netty, aioquic) require the offer.
			tlsCfg.NextProtos = []string{alpn}
		}
		opts.SetTlsConfig(tlsCfg)
	}

	c := client.NewClient(opts)
	received := make(chan []byte, 64)
	c.AddPayloadHandlerLast(func(_ client.Client, m client.Message) {
		received <- m.Payload()
	})

	if t := c.Connect(); t.Wait() && t.Error() != nil {
		fmt.Fprintln(os.Stderr, "connect failed:", t.Error())
		os.Exit(1)
	}
	fmt.Println("CLIENT_CONNECTED")

	waitEcho := func(want []byte, timeout time.Duration) bool {
		deadline := time.After(timeout)
		for {
			select {
			case p := <-received:
				if bytes.Equal(p, want) {
					return true
				}
			case <-deadline:
				return false
			}
		}
	}

	for i := 0; i < count; i++ {
		msg := fmt.Sprintf("ping-%d", i)
		t := c.Push(msg)
		t.WaitTimeout(10 * time.Second)
		if t.Error() != nil {
			fmt.Fprintln(os.Stderr, "push failed:", t.Error())
			os.Exit(1)
		}
		if !waitEcho([]byte("echo:"+msg), 10*time.Second) {
			fmt.Fprintf(os.Stderr, "missing echo for %s\n", msg)
			os.Exit(1)
		}
	}
	fmt.Printf("CLIENT_SEQ_OK n=%d\n", count)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var pushErr error
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			t := c.Push(fmt.Sprintf("conc-%d", i))
			t.WaitTimeout(10 * time.Second)
			if t.Error() != nil {
				mu.Lock()
				pushErr = t.Error()
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if pushErr != nil {
		fmt.Fprintln(os.Stderr, "concurrent push failed:", pushErr)
		os.Exit(1)
	}
	pending := make(map[string]bool, count)
	for i := 0; i < count; i++ {
		pending[fmt.Sprintf("echo:conc-%d", i)] = true
	}
	deadline := time.After(15 * time.Second)
	for len(pending) > 0 {
		select {
		case p := <-received:
			delete(pending, string(p))
		case <-deadline:
			fmt.Fprintf(os.Stderr, "missing concurrent echoes: %v\n", pending)
			os.Exit(1)
		}
	}
	fmt.Printf("CLIENT_CONCURRENT_OK n=%d\n", count)

	blob := make([]byte, binarySize)
	for i := range blob {
		blob[i] = byte(i)
	}
	t := c.Push(blob)
	t.WaitTimeout(20 * time.Second)
	if t.Error() != nil {
		fmt.Fprintln(os.Stderr, "binary push failed:", t.Error())
		os.Exit(1)
	}
	want := append([]byte("echo:"), blob...)
	if !waitEcho(want, 20*time.Second) {
		fmt.Fprintln(os.Stderr, "binary echo mismatch")
		os.Exit(1)
	}
	fmt.Printf("CLIENT_BINARY_OK bytes=%d\n", len(blob))

	t = c.Push([]byte{})
	t.WaitTimeout(10 * time.Second)
	if t.Error() != nil {
		fmt.Fprintln(os.Stderr, "empty push failed:", t.Error())
		os.Exit(1)
	}
	if !waitEcho([]byte("echo:"), 10*time.Second) {
		fmt.Fprintln(os.Stderr, "empty echo missing")
		os.Exit(1)
	}
	fmt.Println("CLIENT_EMPTY_OK")

	c.Disconnect(100)
	fmt.Println("CLIENT_E2E_PASS")
}
