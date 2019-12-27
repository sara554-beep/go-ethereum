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

package lesserver

import (
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/les/lesserver/clientpool"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

var (
	errNoCheckpoint         = errors.New("no local checkpoint provided")
	errNotActivated         = errors.New("checkpoint registrar is not activated")
	errUnknownBenchmarkType = errors.New("unknown benchmark type")
)

// PrivateLightServerAPI provides an API to access the LES light server.
type PrivateLightServerAPI struct {
	server                               *LesServer
	defaultPosFactors, defaultNegFactors clientpool.PriceFactors
}

// NewPrivateLightServerAPI creates a new LES light server API.
func NewPrivateLightServerAPI(server *LesServer) *PrivateLightServerAPI {
	pos, neg := server.clientPool.GetDefaultFactors()
	return &PrivateLightServerAPI{
		server:            server,
		defaultPosFactors: pos,
		defaultNegFactors: neg,
	}
}

// ServerInfo returns global server parameters
func (api *PrivateLightServerAPI) ServerInfo() map[string]interface{} {
	res := make(map[string]interface{})
	res["minimumCapacity"] = api.server.minCapacity
	res["maximumCapacity"] = api.server.maxCapacity
	res["freeClientCapacity"] = api.server.freeCapacity
	res["totalCapacity"], res["totalConnectedCapacity"], res["priorityConnectedCapacity"] = api.server.clientPool.CapacityInfo()
	return res
}

// ClientInfo returns information about clients listed in the ids list or matching the given tags
func (api *PrivateLightServerAPI) ClientInfo(ids []enode.ID) map[enode.ID]map[string]interface{} {
	res := make(map[enode.ID]map[string]interface{})
	api.server.clientPool.ForClients(ids, func(client *clientpool.ClientInfo, id enode.ID) error {
		res[id] = api.clientInfo(client, id)
		return nil
	})
	return res
}

// PriorityClientInfo returns information about clients with a positive balance
// in the given ID range (stop excluded). If stop is null then the iterator stops
// only at the end of the ID space. MaxCount limits the number of results returned.
// If maxCount limit is applied but there are more potential results then the ID
// of the next potential result is included in the map with an empty structure
// assigned to it.
func (api *PrivateLightServerAPI) PriorityClientInfo(start, stop enode.ID, maxCount int) map[enode.ID]map[string]interface{} {
	res := make(map[enode.ID]map[string]interface{})
	ids := api.server.clientPool.GetPosBalanceIDs(start, stop, maxCount+1)
	if len(ids) > maxCount {
		res[ids[maxCount]] = make(map[string]interface{})
		ids = ids[:maxCount]
	}
	if len(ids) != 0 {
		api.server.clientPool.ForClients(ids, func(client *clientpool.ClientInfo, id enode.ID) error {
			res[id] = api.clientInfo(client, id)
			return nil
		})
	}
	return res
}

// clientInfo creates a client info data structure
func (api *PrivateLightServerAPI) clientInfo(c *clientpool.ClientInfo, id enode.ID) map[string]interface{} {
	info := make(map[string]interface{})
	if c != nil {
		now := mclock.Now()
		info["isConnected"] = true
		info["connectionTime"] = float64(now-c.ConnectedAt) / float64(time.Second)
		info["capacity"] = c.Capacity
		pb, nb := c.Balance(now)
		info["pricing/balance"], info["pricing/negBalance"] = pb, nb
		info["pricing/balanceMeta"] = c.BalanceMetaInfo
		info["priority"] = pb != 0
	} else {
		info["isConnected"] = false
		v, m := api.server.clientPool.PoSBalance(id)
		info["pricing/balance"], info["pricing/balanceMeta"] = v, m
		info["priority"] = v != 0
	}
	return info
}

// setParams either sets the given parameters for a single connected client (if specified)
// or the default parameters applicable to clients connected in the future
func (api *PrivateLightServerAPI) setParams(params map[string]interface{}, client *clientpool.ClientInfo, posFactors, negFactors *clientpool.PriceFactors) (updateFactors bool, err error) {
	defParams := client == nil
	for name, value := range params {
		errValue := func() error {
			return fmt.Errorf("invalid value for parameter '%s'", name)
		}
		setFactor := func(v *float64) {
			if val, ok := value.(float64); ok && val >= 0 {
				*v = val / float64(time.Second)
				updateFactors = true
			} else {
				err = errValue()
			}
		}

		switch {
		case name == "pricing/timeFactor":
			setFactor(&posFactors.TimeFactor)
		case name == "pricing/capacityFactor":
			setFactor(&posFactors.CapacityFactor)
		case name == "pricing/requestCostFactor":
			setFactor(&posFactors.RequestFactor)
		case name == "pricing/negative/timeFactor":
			setFactor(&negFactors.TimeFactor)
		case name == "pricing/negative/capacityFactor":
			setFactor(&negFactors.CapacityFactor)
		case name == "pricing/negative/requestCostFactor":
			setFactor(&negFactors.RequestFactor)
		case !defParams && name == "capacity":
			if capacity, ok := value.(float64); ok && uint64(capacity) >= api.server.minCapacity {
				err = api.server.clientPool.SetCapacity(client, uint64(capacity))
				// Don't have to call factor update explicitly. It's already done
				// in setCapacity function.
			} else {
				err = errValue()
			}
		default:
			if defParams {
				err = fmt.Errorf("invalid default parameter '%s'", name)
			} else {
				err = fmt.Errorf("invalid client parameter '%s'", name)
			}
		}
		if err != nil {
			return
		}
	}
	return
}

// AddBalance updates the balance of a client (either overwrites it or adds to it).
// It also updates the balance meta info string.
func (api *PrivateLightServerAPI) AddBalance(id enode.ID, value int64, meta string) ([2]uint64, error) {
	oldBalance, newBalance, err := api.server.clientPool.AddBalance(id, value, meta)
	return [2]uint64{oldBalance, newBalance}, err
}

// SetClientParams sets client parameters for all clients listed in the ids list
// or all connected clients if the list is empty
func (api *PrivateLightServerAPI) SetClientParams(ids []enode.ID, params map[string]interface{}) error {
	return api.server.clientPool.ForClients(ids, func(client *clientpool.ClientInfo, id enode.ID) error {
		if client != nil {
			update, err := api.setParams(params, client, nil, nil)
			if update {
				client.UpdatePriceFactors()
			}
			return err
		} else {
			return fmt.Errorf("client %064x is not connected", id[:])
		}
	})
}

// SetDefaultParams sets the default parameters applicable to clients connected in the future
func (api *PrivateLightServerAPI) SetDefaultParams(params map[string]interface{}) error {
	update, err := api.setParams(params, nil, &api.defaultPosFactors, &api.defaultNegFactors)
	if update {
		api.server.clientPool.SetDefaultFactors(api.defaultPosFactors, api.defaultNegFactors)
	}
	return err
}

// PrivateDebugAPI provides an API to debug LES light server functionality.
type PrivateDebugAPI struct {
	server *LesServer
}

// NewPrivateDebugAPI creates a new LES light server debug API.
func NewPrivateDebugAPI(server *LesServer) *PrivateDebugAPI {
	return &PrivateDebugAPI{
		server: server,
	}
}

// FreezeClient forces a temporary client freeze which normally happens when the server is overloaded
func (api *PrivateDebugAPI) FreezeClient(id enode.ID) error {
	return api.server.clientPool.ForClients([]enode.ID{id}, func(c *clientpool.ClientInfo, id enode.ID) error {
		if c == nil {
			return fmt.Errorf("client %064x is not connected", id[:])
		}
		c.Freeze()
		return nil
	})
}
