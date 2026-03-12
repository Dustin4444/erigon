// Copyright 2015 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

// Contains the meters and timers used by the networking layer.

package p2p

import (
	"net"
	"time"

	"github.com/erigontech/erigon/common/mclock"
	"github.com/erigontech/erigon/diagnostics/metrics"
)

const (
	ingressMeterName = "p2p_ingress"
	egressMeterName  = "p2p_egress"
)

var (
	ingressConnectMeter = metrics.GetOrCreateCounter("p2p_serves")
	ingressTrafficMeter = metrics.GetOrCreateCounter(ingressMeterName)
	egressConnectMeter  = metrics.GetOrCreateCounter("p2p_dials")
	egressTrafficMeter  = metrics.GetOrCreateCounter(egressMeterName)
	activePeerGauge     = metrics.GetOrCreateGauge("p2p_peers")

	// Disconnect reasons — label format: p2p_peer_disconnections_total{reason="requested"}
	peerDisconnectionsTotal = map[string]metrics.Counter{
		"requested":            metrics.GetOrCreateCounter(`p2p_peer_disconnections_total{reason="requested"}`),
		"network_error":        metrics.GetOrCreateCounter(`p2p_peer_disconnections_total{reason="network_error"}`),
		"protocol_error":       metrics.GetOrCreateCounter(`p2p_peer_disconnections_total{reason="protocol_error"}`),
		"useless_peer":         metrics.GetOrCreateCounter(`p2p_peer_disconnections_total{reason="useless_peer"}`),
		"too_many_peers":       metrics.GetOrCreateCounter(`p2p_peer_disconnections_total{reason="too_many_peers"}`),
		"already_connected":    metrics.GetOrCreateCounter(`p2p_peer_disconnections_total{reason="already_connected"}`),
		"incompatible_version": metrics.GetOrCreateCounter(`p2p_peer_disconnections_total{reason="incompatible_version"}`),
		"quitting":             metrics.GetOrCreateCounter(`p2p_peer_disconnections_total{reason="quitting"}`),
		"other":                metrics.GetOrCreateCounter(`p2p_peer_disconnections_total{reason="other"}`),
	}

	// Peer connection duration — how long peers stay connected before disconnecting
	peerConnectionDuration = metrics.GetOrCreateSummary("p2p_peer_connection_duration_seconds")
)

// meteredConn is a wrapper around a net.Conn that meters both the
// inbound and outbound network traffic.
type meteredConn struct {
	net.Conn
}

// newMeteredConn creates a new metered connection, bumps the ingress or egress
// connection meter and also increases the metered peer count. If the metrics
// system is disabled, function returns the original connection.
func newMeteredConn(conn net.Conn, ingress bool, addr *net.TCPAddr) net.Conn {
	// Bump the connection counters and wrap the connection
	if ingress {
		ingressConnectMeter.Inc()
	} else {
		egressConnectMeter.Inc()
	}
	activePeerGauge.Inc()
	return &meteredConn{Conn: conn}
}

// Read delegates a network read to the underlying connection, bumping the common
// and the peer ingress traffic meters along the way.
func (c *meteredConn) Read(b []byte) (n int, err error) {
	n, err = c.Conn.Read(b)
	ingressTrafficMeter.AddInt(n)
	return n, err
}

// Write delegates a network write to the underlying connection, bumping the common
// and the peer egress traffic meters along the way.
func (c *meteredConn) Write(b []byte) (n int, err error) {
	n, err = c.Conn.Write(b)
	egressTrafficMeter.AddInt(n)
	return n, err
}

// Close delegates a close operation to the underlying connection, unregisters
// the peer from the traffic registries and emits close event.
func (c *meteredConn) Close() error {
	err := c.Conn.Close()
	if err == nil {
		activePeerGauge.Dec()
	}
	return err
}

// RecordPeerDisconnect increments the disconnect counter for the given reason
// and observes connection duration. created is the time of connection start.
func RecordPeerDisconnect(reason DiscReason, created mclock.AbsTime) {
	key := discReasonToKey(reason)
	if c, ok := peerDisconnectionsTotal[key]; ok {
		c.Inc()
	} else {
		peerDisconnectionsTotal["other"].Inc()
	}
	durSecs := float64(mclock.Now()-created) / float64(time.Second)
	peerConnectionDuration.Observe(durSecs)
}

func discReasonToKey(r DiscReason) string {
	switch r {
	case DiscRequested:
		return "requested"
	case DiscNetworkError:
		return "network_error"
	case DiscProtocolError:
		return "protocol_error"
	case DiscUselessPeer:
		return "useless_peer"
	case DiscTooManyPeers:
		return "too_many_peers"
	case DiscAlreadyConnected:
		return "already_connected"
	case DiscIncompatibleVersion:
		return "incompatible_version"
	case DiscQuitting:
		return "quitting"
	default:
		return "other"
	}
}
