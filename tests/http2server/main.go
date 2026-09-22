// A real HTTP/2 peer for pytailcat tests. Build against the sibling Tailcat
// module; the first stdin line supplies the test relay region, then EOF exits.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tailscale/tailcat"
	"tailscale.com/envknob"
	"tailscale.com/tailcfg"
	"tailscale.com/types/logger"
)

type signal struct {
	done chan struct{}
	once sync.Once
}

func main() {
	cert := flag.String("cert", "", "TLS certificate")
	key := flag.String("key", "", "TLS private key")
	flag.Parse()
	envknob.Setenv("IN_TS_TEST", "true")
	input := bufio.NewReader(os.Stdin)
	line, err := input.ReadBytes('\n')
	check(err)
	var region tailcfg.DERPRegion
	check(json.Unmarshal(line, &region))
	peer := &tailcat.Server{Region: &region, Logf: logger.Discard}
	defer peer.Close()
	plain, err := peer.Listen(context.Background(), "tcp", ":0")
	check(err)
	secure, err := peer.Listen(context.Background(), "tcp", ":0")
	check(err)

	var signals sync.Map
	getSignal := func(token string) *signal {
		value, _ := signals.LoadOrStore(token, &signal{done: make(chan struct{})})
		return value.(*signal)
	}
	type connKey struct{}
	var nextID atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Connection-ID", fmt.Sprint(r.Context().Value(connKey{})))
		w.Header().Set("X-HTTP-Protocol", r.Proto)
		w.Header().Set("X-Request-Host", r.Host)
		if r.TLS != nil {
			w.Header().Set("X-TLS-ALPN", r.TLS.NegotiatedProtocol)
			w.Header().Set("X-TLS-SNI", r.TLS.ServerName)
		}
		switch r.URL.Path {
		case "/wait":
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			select {
			case <-getSignal(r.URL.Query().Get("token")).done:
				io.WriteString(w, "released")
			case <-r.Context().Done():
			}
		case "/release":
			s := getSignal(r.URL.Query().Get("token"))
			s.once.Do(func() { close(s.done) })
			io.WriteString(w, "ok")
		case "/slow":
			select {
			case <-time.After(3 * time.Second):
				io.WriteString(w, "ok")
			case <-r.Context().Done():
			}
		case "/reset":
			panic(http.ErrAbortHandler)
		case "/stream":
			for range 16 {
				if _, err := w.Write(make([]byte, 16384)); err != nil {
					return
				}
				w.(http.Flusher).Flush()
				time.Sleep(5 * time.Millisecond)
			}
		default:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				return
			}
			w.Write(body)
		}
	})
	protocols := &http.Protocols{}
	protocols.SetHTTP1(true)
	protocols.SetHTTP2(true)
	protocols.SetUnencryptedHTTP2(true)
	makeServer := func() *http.Server {
		return &http.Server{
			Handler: handler, Protocols: protocols, ErrorLog: logger.StdLogger(logger.Discard),
			ConnContext: func(ctx context.Context, _ net.Conn) context.Context {
				return context.WithValue(ctx, connKey{}, nextID.Add(1))
			},
		}
	}
	plainServer, secureServer := makeServer(), makeServer()
	defer plainServer.Close()
	defer secureServer.Close()
	go plainServer.Serve(plain)
	go secureServer.ServeTLS(secure, *cert, *key)
	localPlain, err := net.Listen("tcp", "127.0.0.1:0")
	check(err)
	localSecure, err := net.Listen("tcp", "127.0.0.1:0")
	check(err)
	localPlainServer, localSecureServer := makeServer(), makeServer()
	defer localPlainServer.Close()
	defer localSecureServer.Close()
	go localPlainServer.Serve(localPlain)
	go localSecureServer.ServeTLS(localSecure, *cert, *key)
	port := func(ln net.Listener) int {
		_, p, _ := net.SplitHostPort(ln.Addr().String())
		n, _ := strconv.Atoi(p)
		return n
	}
	check(json.NewEncoder(os.Stdout).Encode(map[string]any{
		"address": peer.TailcatAddr(), "http_port": port(plain), "https_port": port(secure),
		"local_http_port": port(localPlain), "local_https_port": port(localSecure),
	}))
	io.Copy(io.Discard, input)
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
