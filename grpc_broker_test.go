// Copyright IBM Corp. 2016, 2025
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"errors"
	"io"
	"net"
	"os"
	"runtime/debug"
	"testing"
	"time"

	"github.com/hashicorp/go-plugin/internal/grpcmux"
	"github.com/hashicorp/go-plugin/internal/plugin"
	"github.com/hashicorp/go-plugin/runner"
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
			b := newGRPCBroker(s, nil, brokerTestSocketConfig(t), translator, muxer)
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
	b := newGRPCBroker(s, nil, brokerTestSocketConfig(t), nil, muxer)
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
