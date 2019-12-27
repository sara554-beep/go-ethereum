// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package metrics

import (
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/p2p"
)

var (
	MiscInPacketsMeter           = metrics.NewRegisteredMeter("les/misc/in/packets/total", nil)
	MiscInTrafficMeter           = metrics.NewRegisteredMeter("les/misc/in/traffic/total", nil)
	MiscInHeaderPacketsMeter     = metrics.NewRegisteredMeter("les/misc/in/packets/header", nil)
	MiscInHeaderTrafficMeter     = metrics.NewRegisteredMeter("les/misc/in/traffic/header", nil)
	MiscInBodyPacketsMeter       = metrics.NewRegisteredMeter("les/misc/in/packets/body", nil)
	MiscInBodyTrafficMeter       = metrics.NewRegisteredMeter("les/misc/in/traffic/body", nil)
	MiscInCodePacketsMeter       = metrics.NewRegisteredMeter("les/misc/in/packets/code", nil)
	MiscInCodeTrafficMeter       = metrics.NewRegisteredMeter("les/misc/in/traffic/code", nil)
	MiscInReceiptPacketsMeter    = metrics.NewRegisteredMeter("les/misc/in/packets/receipt", nil)
	MiscInReceiptTrafficMeter    = metrics.NewRegisteredMeter("les/misc/in/traffic/receipt", nil)
	MiscInTrieProofPacketsMeter  = metrics.NewRegisteredMeter("les/misc/in/packets/proof", nil)
	MiscInTrieProofTrafficMeter  = metrics.NewRegisteredMeter("les/misc/in/traffic/proof", nil)
	MiscInHelperTriePacketsMeter = metrics.NewRegisteredMeter("les/misc/in/packets/helperTrie", nil)
	MiscInHelperTrieTrafficMeter = metrics.NewRegisteredMeter("les/misc/in/traffic/helperTrie", nil)
	MiscInTxsPacketsMeter        = metrics.NewRegisteredMeter("les/misc/in/packets/txs", nil)
	MiscInTxsTrafficMeter        = metrics.NewRegisteredMeter("les/misc/in/traffic/txs", nil)
	MiscInTxStatusPacketsMeter   = metrics.NewRegisteredMeter("les/misc/in/packets/txStatus", nil)
	MiscInTxStatusTrafficMeter   = metrics.NewRegisteredMeter("les/misc/in/traffic/txStatus", nil)

	MiscOutPacketsMeter           = metrics.NewRegisteredMeter("les/misc/out/packets/total", nil)
	MiscOutTrafficMeter           = metrics.NewRegisteredMeter("les/misc/out/traffic/total", nil)
	MiscOutHeaderPacketsMeter     = metrics.NewRegisteredMeter("les/misc/out/packets/header", nil)
	MiscOutHeaderTrafficMeter     = metrics.NewRegisteredMeter("les/misc/out/traffic/header", nil)
	MiscOutBodyPacketsMeter       = metrics.NewRegisteredMeter("les/misc/out/packets/body", nil)
	MiscOutBodyTrafficMeter       = metrics.NewRegisteredMeter("les/misc/out/traffic/body", nil)
	MiscOutCodePacketsMeter       = metrics.NewRegisteredMeter("les/misc/out/packets/code", nil)
	MiscOutCodeTrafficMeter       = metrics.NewRegisteredMeter("les/misc/out/traffic/code", nil)
	MiscOutReceiptPacketsMeter    = metrics.NewRegisteredMeter("les/misc/out/packets/receipt", nil)
	MiscOutReceiptTrafficMeter    = metrics.NewRegisteredMeter("les/misc/out/traffic/receipt", nil)
	MiscOutTrieProofPacketsMeter  = metrics.NewRegisteredMeter("les/misc/out/packets/proof", nil)
	MiscOutTrieProofTrafficMeter  = metrics.NewRegisteredMeter("les/misc/out/traffic/proof", nil)
	MiscOutHelperTriePacketsMeter = metrics.NewRegisteredMeter("les/misc/out/packets/helperTrie", nil)
	MiscOutHelperTrieTrafficMeter = metrics.NewRegisteredMeter("les/misc/out/traffic/helperTrie", nil)
	MiscOutTxsPacketsMeter        = metrics.NewRegisteredMeter("les/misc/out/packets/txs", nil)
	MiscOutTxsTrafficMeter        = metrics.NewRegisteredMeter("les/misc/out/traffic/txs", nil)
	MiscOutTxStatusPacketsMeter   = metrics.NewRegisteredMeter("les/misc/out/packets/txStatus", nil)
	MiscOutTxStatusTrafficMeter   = metrics.NewRegisteredMeter("les/misc/out/traffic/txStatus", nil)

	MiscServingTimeHeaderTimer     = metrics.NewRegisteredTimer("les/misc/serve/header", nil)
	MiscServingTimeBodyTimer       = metrics.NewRegisteredTimer("les/misc/serve/body", nil)
	MiscServingTimeCodeTimer       = metrics.NewRegisteredTimer("les/misc/serve/code", nil)
	MiscServingTimeReceiptTimer    = metrics.NewRegisteredTimer("les/misc/serve/receipt", nil)
	MiscServingTimeTrieProofTimer  = metrics.NewRegisteredTimer("les/misc/serve/proof", nil)
	MiscServingTimeHelperTrieTimer = metrics.NewRegisteredTimer("les/misc/serve/helperTrie", nil)
	MiscServingTimeTxTimer         = metrics.NewRegisteredTimer("les/misc/serve/txs", nil)
	MiscServingTimeTxStatusTimer   = metrics.NewRegisteredTimer("les/misc/serve/txStatus", nil)

	ConnectionTimer       = metrics.NewRegisteredTimer("les/connection/duration", nil)
	ServerConnectionGauge = metrics.NewRegisteredGauge("les/connection/server", nil)
	ClientConnectionGauge = metrics.NewRegisteredGauge("les/connection/client", nil)

	TotalCapacityGauge   = metrics.NewRegisteredGauge("les/server/totalCapacity", nil)
	TotalRechargeGauge   = metrics.NewRegisteredGauge("les/server/totalRecharge", nil)
	TotalConnectedGauge  = metrics.NewRegisteredGauge("les/server/totalConnected", nil)
	BlockProcessingTimer = metrics.NewRegisteredTimer("les/server/blockProcessingTime", nil)

	RequestServedMeter               = metrics.NewRegisteredMeter("les/server/req/avgServedTime", nil)
	RequestServedTimer               = metrics.NewRegisteredTimer("les/server/req/servedTime", nil)
	RequestEstimatedMeter            = metrics.NewRegisteredMeter("les/server/req/avgEstimatedTime", nil)
	RequestEstimatedTimer            = metrics.NewRegisteredTimer("les/server/req/estimatedTime", nil)
	RelativeCostHistogram            = metrics.NewRegisteredHistogram("les/server/req/relative", nil, metrics.NewExpDecaySample(1028, 0.015))
	RelativeCostHeaderHistogram      = metrics.NewRegisteredHistogram("les/server/req/relative/header", nil, metrics.NewExpDecaySample(1028, 0.015))
	RelativeCostBodyHistogram        = metrics.NewRegisteredHistogram("les/server/req/relative/body", nil, metrics.NewExpDecaySample(1028, 0.015))
	RelativeCostReceiptHistogram     = metrics.NewRegisteredHistogram("les/server/req/relative/receipt", nil, metrics.NewExpDecaySample(1028, 0.015))
	RelativeCostCodeHistogram        = metrics.NewRegisteredHistogram("les/server/req/relative/code", nil, metrics.NewExpDecaySample(1028, 0.015))
	RelativeCostProofHistogram       = metrics.NewRegisteredHistogram("les/server/req/relative/proof", nil, metrics.NewExpDecaySample(1028, 0.015))
	RelativeCostHelperProofHistogram = metrics.NewRegisteredHistogram("les/server/req/relative/helperTrie", nil, metrics.NewExpDecaySample(1028, 0.015))
	RelativeCostSendTxHistogram      = metrics.NewRegisteredHistogram("les/server/req/relative/txs", nil, metrics.NewExpDecaySample(1028, 0.015))
	RelativeCostTxStatusHistogram    = metrics.NewRegisteredHistogram("les/server/req/relative/txStatus", nil, metrics.NewExpDecaySample(1028, 0.015))

	GlobalFactorGauge    = metrics.NewRegisteredGauge("les/server/globalFactor", nil)
	RecentServedGauge    = metrics.NewRegisteredGauge("les/server/recentRequestServed", nil)
	RecentEstimatedGauge = metrics.NewRegisteredGauge("les/server/recentRequestEstimated", nil)
	SqServedGauge        = metrics.NewRegisteredGauge("les/server/servingQueue/served", nil)
	SqQueuedGauge        = metrics.NewRegisteredGauge("les/server/servingQueue/queued", nil)

	ClientConnectedMeter    = metrics.NewRegisteredMeter("les/server/clientEvent/connected", nil)
	ClientRejectedMeter     = metrics.NewRegisteredMeter("les/server/clientEvent/rejected", nil)
	ClientKickedMeter       = metrics.NewRegisteredMeter("les/server/clientEvent/kicked", nil)
	ClientDisconnectedMeter = metrics.NewRegisteredMeter("les/server/clientEvent/disconnected", nil)
	ClientFreezeMeter       = metrics.NewRegisteredMeter("les/server/clientEvent/freeze", nil)
	ClientErrorMeter        = metrics.NewRegisteredMeter("les/server/clientEvent/error", nil)

	RequestRTT       = metrics.NewRegisteredTimer("les/client/req/rtt", nil)
	RequestSendDelay = metrics.NewRegisteredTimer("les/client/req/sendDelay", nil)
)

// MeteredMsgReadWriter is a wrapper around a p2p.MsgReadWriter, capable of
// accumulating the above defined metrics based on the data stream contents.
type MeteredMsgReadWriter struct {
	p2p.MsgReadWriter     // Wrapped message stream to meter
	version           int // Protocol version to select correct meters
}

// NewMeteredMsgWriter wraps a p2p MsgReadWriter with metering support. If the
// metrics system is disabled, this function returns the original object.
func NewMeteredMsgWriter(rw p2p.MsgReadWriter, version int) p2p.MsgReadWriter {
	if !metrics.Enabled {
		return rw
	}
	return &MeteredMsgReadWriter{MsgReadWriter: rw, version: version}
}

func (rw *MeteredMsgReadWriter) ReadMsg() (p2p.Msg, error) {
	// Read the message and short circuit in case of an error
	msg, err := rw.MsgReadWriter.ReadMsg()
	if err != nil {
		return msg, err
	}
	// Account for the data traffic
	packets, traffic := MiscInPacketsMeter, MiscInTrafficMeter
	packets.Mark(1)
	traffic.Mark(int64(msg.Size))

	return msg, err
}

func (rw *MeteredMsgReadWriter) WriteMsg(msg p2p.Msg) error {
	// Account for the data traffic
	packets, traffic := MiscOutPacketsMeter, MiscOutTrafficMeter
	packets.Mark(1)
	traffic.Mark(int64(msg.Size))

	// Send the packet to the p2p layer
	return rw.MsgReadWriter.WriteMsg(msg)
}
