// Copyright 2019 The go-ethereum Authors
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

package protocol

type (
	// RequestCostTable assigns a cost estimate function to each request type
	// which is a linear function of the requested amount
	// (cost = BaseCost + ReqCost * amount)
	RequestCostTable map[uint64]*RequestCosts
	RequestCosts     struct {
		BaseCost, ReqCost uint64
	}

	// RequestCostList is a list representation of request costs which is used for
	// database storage and communication through the network
	RequestCostList     []RequestCostListItem
	RequestCostListItem struct {
		MsgCode, BaseCost, ReqCost uint64
	}
)

// GetMaxCost calculates the estimated cost for a given request type and amount
func (table RequestCostTable) GetMaxCost(code, amount uint64) uint64 {
	costs := table[code]
	return costs.BaseCost + amount*costs.ReqCost
}

// Decode converts a cost list to a cost table
func (list RequestCostList) Decode(protocolLength uint64) RequestCostTable {
	table := make(RequestCostTable)
	for _, e := range list {
		if e.MsgCode < protocolLength {
			table[e.MsgCode] = &RequestCosts{
				BaseCost: e.BaseCost,
				ReqCost:  e.ReqCost,
			}
		}
	}
	return table
}

//// TestCostList returns a dummy request cost list used by tests
//func TestCostList(testCost uint64) RequestCostList {
//	cl := make(RequestCostList, len(reqAvgTimeCost))
//	var max uint64
//	for code := range reqAvgTimeCost {
//		if code > max {
//			max = code
//		}
//	}
//	i := 0
//	for code := uint64(0); code <= max; code++ {
//		if _, ok := reqAvgTimeCost[code]; ok {
//			cl[i].MsgCode = code
//			cl[i].BaseCost = testCost
//			cl[i].ReqCost = 0
//			i++
//		}
//	}
//	return cl
//}
