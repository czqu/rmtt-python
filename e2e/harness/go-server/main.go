// Command server is the Go side of the rmtt-python cross-language E2E matrix.
//
// Usage: server <scheme> <port> [certFile keyFile] [--reject CRED] [--kp-min N] [--kp-max N]
//
// Prints SERVER_READY <scheme> <port> once bound and echoes every upstream
// PUSH back as "echo:<payload>".
package main

import (
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/czqu/rmtt-go/server"
)

type allowAll struct{}

func (allowAll) Authenticate(credential string) (string, bool) { return credential, true }

type rejectOne struct{ credential string }

func (r rejectOne) Authenticate(credential string) (string, bool) {
	if credential == r.credential {
		return "", false
	}
	return credential, true
}

func main() {
	var positional []string
	reject := ""
	kpMin, kpMax := int64(30), int64(600)
	args := os.Args[1:]
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--reject":
			if i+1 < len(args) {
				reject = args[i+1]
				i++
			}
		case "--kp-min":
			if i+1 < len(args) {
				kpMin, _ = strconv.ParseInt(args[i+1], 10, 64)
				i++
			}
		case "--kp-max":
			if i+1 < len(args) {
				kpMax, _ = strconv.ParseInt(args[i+1], 10, 64)
				i++
			}
		default:
			positional = append(positional, args[i])
		}
	}
	if len(positional) < 2 {
		fmt.Fprintln(os.Stderr, "usage: server <scheme> <port> [cert key] [--reject CRED] [--kp-min N] [--kp-max N]")
		os.Exit(2)
	}
	scheme := positional[0]
	port := positional[1]
	addr := ":" + port

	var tlsConfig *tls.Config
	if len(positional) >= 4 {
		cert, err := tls.LoadX509KeyPair(positional[2], positional[3])
		if err != nil {
			fmt.Fprintln(os.Stderr, "load cert:", err)
			os.Exit(1)
		}
		tlsConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
	}

	var listener server.Listener
	switch scheme {
	case "tcp":
		listener = server.NewTCPListener(addr)
	case "tls":
		listener = server.NewTLSListener(addr, tlsConfig)
	case "ws":
		listener = server.NewWSListener(addr, "/rmtt")
	case "wss":
		listener = server.NewWSSListener(addr, "/rmtt", tlsConfig)
	case "kcp":
		listener = server.NewKCPListener(addr)
	case "quic":
		listener = server.NewQUICListener(addr, tlsConfig)
	default:
		fmt.Fprintln(os.Stderr, "unsupported scheme:", scheme)
		os.Exit(2)
	}

	var srv server.Server
	opts := server.NewServerOptions()
	opts.AddListener(listener)
	if reject != "" {
		opts.SetAuthenticator(rejectOne{credential: reject})
	} else {
		opts.SetAuthenticator(allowAll{})
	}
	opts.SetKeepalivePolicy(&server.KeepalivePolicy{
		MinSeconds: kpMin, MaxSeconds: kpMax, DefaultSeconds: 60,
	})
	opts.SetMessageHandler(func(deviceID string, payload []byte) {
		fmt.Printf("PUSH device=%s len=%d\n", deviceID, len(payload))
		echo := append([]byte("echo:"), payload...)
		if err := srv.Push(deviceID, echo); err != nil {
			fmt.Fprintf(os.Stderr, "echo push failed: %v\n", err)
		}
	})
	srv = server.NewServer(opts)

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
		<-sig
		_ = srv.Close()
		os.Exit(0)
	}()

	go func() {
		if err := srv.ListenAndServe(); err != nil && err.Error() != "context canceled" {
			fmt.Fprintln(os.Stderr, "serve:", err)
			os.Exit(1)
		}
	}()
	// Wait for the listener to be accepting before advertising readiness:
	// ListenAndServe binds asynchronously, so an immediate READY races with it.
	switch scheme {
	case "tcp", "tls", "ws", "wss":
		for i := 0; i < 100; i++ {
			conn, err := net.DialTimeout("tcp", "127.0.0.1:"+port, 100*time.Millisecond)
			if err == nil {
				_ = conn.Close()
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	default:
		time.Sleep(400 * time.Millisecond)
	}
	fmt.Printf("SERVER_READY %s %s\n", scheme, port)
	select {}
}
