// Copyright 2017 The go-ethereum Authors
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

package rawdb

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"testing"
)

func TestFoo(t *testing.T) {
	owner := common.HexToHash("76e83933629aac570bd42095dd3f7418adacc5cd156cd11aa9f66dddc16d72e5")
	key := storageTrieNodeKey(owner, []byte{0xd, 0xe, 0x3, 0xd, 0x4})
	fmt.Println(hexutil.Encode(key))

	blob := common.FromHex("0xf901f1a0f66a9895c4e786990af28d63444b7015b91ba4061aaea4fe438fd9b748fad1b3a0fc9fdbaabe3dc0baab436e2ecea6ddcc4cfb1a00e1a991312f6727c23fff5842a0505f7e33dc679749fb99a9d797ed226a475b521feaeb0725296f50ac2af95325a0ce09a797f9065d2f4ed3daae7b286ec69d3ac69aaa8ae27ff1d88fd8445ea7ada03be61670fadf9debf520468a8e86d615068287984ab82982e02c1315348f7908a048f99ad6e1b7ef75d6fa14600c16446d945a34ed2bb9fccc29a0bedd0ffe6108a0ebdd529a9c8129f00140ca4ac47a9ccab0a23c4c131aa7285720147898fca778a06af528f8a55044377fdb5a37a9ff459ad6109db1bebc1b8ef9d492df61515aaba0055c9a8467e2ddb840c08c92638bbf7066f3dba71307ab6594179ff9221d323ba0bbcdaf311004e79639ad01dc868d896ee4beac9a2dff86c1d9c0114ce98b00f6a06e348e15714748044154756933e173c0d15627fb65808ec04ea8d50d0dfc8273a004f9f99861cc74804093f04733b5d1d21fade8e8c3dcf2a53ace91962507cd72a0b345ef9ef61d7e393cc479bf5abb64dd1cdd514c854aee76f1d1e9b8218f998d80a0a3dee7fa630bba136757209656c6a209859c2ef59b446222087c62e42da24db6a0bfc84f0a8daaba35c003ce2729cefb88ce54608f677d0f8058da955b0225806280")
	fmt.Println(crypto.Keccak256Hash(blob).Hex())

	fmt.Println(crypto.Keccak256Hash(common.FromHex("0x095ddce4fd8818ad159d778e6a9898a2474933ca")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0x51bd43485cda9255c78bfa9dd17f5ee063ee9d66")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0x31c563632a51cb0f9c481e4502c13b11949d9554")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xe39b59f94001c648ddb0ad770ab5a2d2e475f4b7")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xf458747c6d6db57970de80da6474c0a3dfe94bf1")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xff00000000000000000000000000000000042069")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0x44e63823393b8c3cc39347139707944bb88b9e98")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xb20bb9105e007bd3e0f73d63d4d3da2c8f736b77")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xc1cb36667c04d10ea5e035bf3cbfb82f20ab78c6")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xdb7ce68f0123badb42f0783376249ab8271473ce")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xebdb8213043b3908d1a461bd2c6e65267473e1dc")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xff00000000000000000000000000000000032001")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xb20bb9105e007bd3e0f73d63d4d3da2c8f736b77")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xb20bb9105e007bd3e0f73d63d4d3da2c8f736b77")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0x2f6a076e89b3df6d76015caeb529e4997673e659")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xff00000000000000000000000000000000017001")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xb20bb9105e007bd3e0f73d63d4d3da2c8f736b77")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xff00000000000000000000000000000000042069")).Hex())
	fmt.Println(crypto.Keccak256Hash(common.FromHex("0xf458747c6d6db57970de80da6474c0a3dfe94bf1")).Hex())
}
