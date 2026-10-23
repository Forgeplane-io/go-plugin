// Copyright IBM Corp. 2016, 2025
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin/internal/grpcmux"
	"github.com/hashicorp/go-plugin/internal/plugin"
	"github.com/hashicorp/go-plugin/runner"
	"google.golang.org/grpc"
)

func TestGRPCBroker_AcceptAdvertisementError(t *testing.T) {
	for _, translationError := range []bool{false, true} {
		name := "send"
		if translationError {
			name = "translation"
		}
		t.Run(name, func(t *testing.T) {
			// Don't let a net.Listener finalizer hide a leaked listener.
			gcPercent := debug.SetGCPercent(-1)
			defer debug.SetGCPercent(gcPercent)

			wantErr := errors.New("advertisement failed")
			var network, address string
			var peer net.Conn
			observeListener := func(n, a string) {
				t.Helper()
				network, address = n, a
				var err error
				peer, err = net.DialTimeout(network, address, time.Second)
				if err != nil {
					t.Fatalf("listener was not open before advertisement: %s", err)
				}
				t.Cleanup(func() { _ = peer.Close() })
			}

			sendCalled := false
			s := &brokerTestStreamer{send: func(info *plugin.ConnInfo) error {
				sendCalled = true
				observeListener(info.Network, info.Address)
				return wantErr
			}}
			var translator runner.AddrTranslator
			if translationError {
				translator = &brokerTestAddrTranslator{hostToPlugin: func(n, a string) (string, string, error) {
					observeListener(n, a)
					return "", "", wantErr
				}}
			}
			var muxer *grpcmux.GRPCClientMuxer
			b := newGRPCBroker(nil, s, nil, brokerTestSocketConfig(t), translator, muxer)
			ln, err := b.Accept(1)
			if ln != nil {
				_ = ln.Close()
				t.Fatal("Accept returned a listener on advertisement failure")
			}
			if !errors.Is(err, wantErr) {
				t.Fatalf("expected advertisement error, got %v", err)
			}
			if sendCalled == translationError {
				t.Errorf("Send called = %t, translation error = %t", sendCalled, translationError)
			}

			if network == "unix" {
				if _, err := os.Stat(address); !os.IsNotExist(err) {
					t.Errorf("listener socket still exists after failed advertisement: %v", err)
				}
			}
			// A connection queued on the listener must also be closed, proving
			// the listener itself was closed, not just its socket path removed.
			if err := peer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err := peer.Read(make([]byte, 1)); err == nil {
				t.Error("unexpected data from unaccepted connection")
			} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				t.Error("listener still holds an unaccepted connection after failed advertisement")
			}
		})
	}
}

func TestGRPCBroker_AcceptListenerOwnership(t *testing.T) {
	var info *plugin.ConnInfo
	s := &brokerTestStreamer{send: func(i *plugin.ConnInfo) error {
		info = i
		return nil
	}}
	var muxer *grpcmux.GRPCClientMuxer
	b := newGRPCBroker(nil, s, nil, brokerTestSocketConfig(t), nil, muxer)
	ln, err := b.Accept(1)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	if info == nil || info.ServiceId != 1 || info.Network != ln.Addr().Network() || info.Address != ln.Addr().String() {
		t.Fatalf("unexpected advertisement: %v", info)
	}
	conn, err := net.DialTimeout(info.Network, info.Address, time.Second)
	if err != nil {
		t.Fatalf("Accept closed a successfully advertised listener: %s", err)
	}
	defer conn.Close()
	accepted, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer accepted.Close()
}

// Keep Unix socket paths short enough for platforms with small sockaddr_un limits.
func brokerTestSocketConfig(t *testing.T) UnixSocketConfig {
	t.Helper()
	dir, err := os.MkdirTemp("", "broker")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return UnixSocketConfig{socketDir: dir}
}

type brokerTestStreamer struct {
	send func(*plugin.ConnInfo) error
}

func (s *brokerTestStreamer) Send(info *plugin.ConnInfo) error { return s.send(info) }
func (s *brokerTestStreamer) Recv() (*plugin.ConnInfo, error)  { return nil, io.EOF }
func (s *brokerTestStreamer) Close()                           {}

type brokerTestAddrTranslator struct {
	hostToPlugin func(string, string) (string, string, error)
}

func (t *brokerTestAddrTranslator) HostToPlugin(n, a string) (string, string, error) {
	return t.hostToPlugin(n, a)
}

func (t *brokerTestAddrTranslator) PluginToHost(n, a string) (string, string, error) {
	return n, a, nil
}

func TestGRPCBroker_DefaultLogger(t *testing.T) {
	b := newGRPCBroker(nil, nil, nil, UnixSocketConfig{}, nil, nil)
	if b.logger == nil || b.logger.Name() != "plugin" || b.logger.GetLevel() != hclog.Trace {
		t.Fatalf("unexpected default broker logger: %#v", b.logger)
	}
}

func TestGRPCBroker_AcceptAndServeLogger(t *testing.T) {
	for _, owner := range []string{"client", "server"} {
		for _, level := range []hclog.Level{hclog.Error, hclog.Off} {
			t.Run(owner+"/"+level.String(), func(t *testing.T) {
				output := make(brokerTestLogWriter, 16)
				logger := hclog.New(&hclog.LoggerOptions{Level: level, Output: output, JSONFormat: true})
				b := brokerTestOwnedBroker(t, owner, logger, false)
				stdlib := make(brokerTestLogWriter, 16)
				oldOutput := log.Writer()
				log.SetOutput(stdlib)
				defer log.SetOutput(oldOutput)

				if err := b.Close(); err != nil {
					t.Fatal(err)
				}
				b.AcceptAndServe(1, func(opts []grpc.ServerOption) *grpc.Server {
					t.Error("server factory called after failed Accept")
					return grpc.NewServer(opts...)
				})

				select {
				case entry := <-stdlib:
					t.Errorf("unexpected stdlib log output: %s", entry)
				default:
				}
				select {
				case entry := <-output:
					if level == hclog.Off {
						t.Errorf("disabled logger received output: %s", entry)
					} else {
						brokerTestErrorLog(t, entry, "plugin acceptAndServe error", "broker closed")
					}
				default:
					if level != hclog.Off {
						t.Error("broker error did not reach the owning hclog logger")
					}
				}
			})
		}
	}
}

func TestGRPCBroker_KnockErrorLogger(t *testing.T) {
	for _, owner := range []string{"client", "server"} {
		t.Run(owner, func(t *testing.T) {
			output := make(brokerTestLogWriter, 16)
			logger := hclog.New(&hclog.LoggerOptions{Level: hclog.Error, Output: output, JSONFormat: true})
			b := brokerTestOwnedBroker(t, owner, logger, true)
			stdlib := make(brokerTestLogWriter, 16)
			oldOutput := log.Writer()
			log.SetOutput(stdlib)
			defer log.SetOutput(oldOutput)

			// An invalid knock exercises the asynchronous broker error path
			// without opening a connection on this listener.
			const id = 42
			ln, err := b.Accept(id)
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			b.getServerStream(id).ch <- &plugin.ConnInfo{ServiceId: id}

			select {
			case entry := <-output:
				fields := brokerTestErrorLog(t, entry, "error listening for knocks", "knock received for service ID 42 with incorrect values; knock=<nil>")
				if fields["id"] != float64(id) {
					t.Errorf("knock log lost stream ID: %s", entry)
				}
			case entry := <-stdlib:
				t.Errorf("broker error went to stdlib instead of hclog: %s", entry)
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for knock error log")
			}
			select {
			case entry := <-stdlib:
				t.Errorf("unexpected stdlib log output: %s", entry)
			default:
			}
		})
	}
}

// Exercise the real client and server construction sites with distinct loggers.
func brokerTestOwnedBroker(t *testing.T, owner string, logger hclog.Logger, multiplex bool) *GRPCBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	clientLogger, serverLogger := hclog.NewNullLogger(), hclog.NewNullLogger()
	if owner == "client" {
		clientLogger = logger
	} else {
		serverLogger = logger
	}
	var muxer *grpcmux.GRPCServerMuxer
	var serverListener net.Listener = ln
	if multiplex {
		muxer = grpcmux.NewGRPCServerMuxer(serverLogger, ln)
		serverListener = muxer
	}
	s := &GRPCServer{
		Server: DefaultGRPCServer,
		DoneCh: make(chan struct{}),
		Stdout: strings.NewReader(""),
		Stderr: strings.NewReader(""),
		logger: serverLogger,
		muxer:  muxer,
	}
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Stop)
	go s.Serve(serverListener)
	c := NewClient(&ClientConfig{Logger: clientLogger, GRPCBrokerMultiplex: multiplex})
	c.address, c.protocol = ln.Addr(), ProtocolGRPC
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	client, err := newGRPCClient(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.broker.Close()
		_ = client.Conn.Close()
		if multiplex {
			_ = c.grpcMuxer.Close()
		}
	})
	b := s.broker
	if owner == "client" {
		b = client.broker
	}
	b.unixSocketCfg = brokerTestSocketConfig(t)
	return b
}

type brokerTestLogWriter chan string

func (w brokerTestLogWriter) Write(p []byte) (int, error) {
	w <- string(p)
	return len(p), nil
}

func brokerTestErrorLog(t *testing.T, entry, message, errText string) map[string]interface{} {
	t.Helper()
	var fields map[string]interface{}
	if err := json.Unmarshal([]byte(entry), &fields); err != nil {
		t.Fatalf("invalid hclog JSON: %s", err)
	}
	if fields["@level"] != "error" || fields["@message"] != message || fields["error"] != errText {
		t.Errorf("unexpected broker error log: %s", entry)
	}
	return fields
}
