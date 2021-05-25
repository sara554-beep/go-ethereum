// Copyright 2021 The go-ethereum Authors
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
	"bytes"
	"fmt"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/rlp"
)

// freezerTableBatch is a batch for a freezer table.
type freezerTableBatch struct {
	t   *freezerTable
	buf bytes.Buffer
	sb  *BufferedSnapWriter

	firstIdx uint64
	count    uint64
	sizes    []uint32

	headBytes uint32
}

// NewBatch creates a new batch for the freezer table.
func (t *freezerTable) NewBatch() *freezerTableBatch {
	batch := &freezerTableBatch{
		t:        t,
		firstIdx: math.MaxUint64,
	}
	if !t.noCompression {
		batch.sb = new(BufferedSnapWriter)
	}
	return batch
}

// AppendRLP rlp-encodes and adds data at the end of the freezer table. The item number
// is a precautionary parameter to ensure data correctness, but the table will
// reject already existing data.
func (batch *freezerTableBatch) AppendRLP(item uint64, data interface{}) error {
	if batch.firstIdx == math.MaxUint64 {
		batch.firstIdx = item
	}
	if have, want := item, batch.firstIdx+batch.count; have != want {
		return fmt.Errorf("appending unexpected item: want %d, have %d", want, have)
	}
	s0 := batch.buf.Len()
	if batch.sb != nil {
		// RLP-encode
		rlp.Encode(batch.sb, data)
		// Pass to
		batch.sb.WriteTo(&batch.buf)
	} else {
		rlp.Encode(&batch.buf, data)
	}
	s1 := batch.buf.Len()
	batch.sizes = append(batch.sizes, uint32(s1-s0))
	batch.count++
	return nil
}

// Append injects a binary blob at the end of the freezer table. The item number
// is a precautionary parameter to ensure data correctness, but the table will
// reject already existing data.
func (batch *freezerTableBatch) Append(item uint64, blob []byte) error {
	if batch.firstIdx == math.MaxUint64 {
		batch.firstIdx = item
	}
	if have, want := item, batch.firstIdx+batch.count; have != want {
		return fmt.Errorf("appending unexpected item: want %d, have %d", want, have)
	}
	s0 := batch.buf.Len()
	if batch.sb != nil {
		batch.sb.WriteDirectTo(&batch.buf, blob)
	} else {
		batch.buf.Write(blob)
	}
	s1 := batch.buf.Len()
	batch.sizes = append(batch.sizes, uint32(s1-s0))
	batch.count++
	return nil
}

// Write writes the batched items to the backing freezerTable.
func (batch *freezerTableBatch) Write() error {
	var (
		retry = false
		err   error
	)
	for {
		retry, err = batch.write(retry)
		if err != nil {
			return err
		}
		if !retry {
			return nil
		}
	}
}

// write is the internal implementation which writes the batch to the backing
// table. It will only ever write as many items as fits into one table: if
// the backing table needs to open a new file, this method will return with a
// (true, nil), to signify that it needs to be invoked again.
func (batch *freezerTableBatch) write(newHead bool) (bool, error) {
	if !newHead {
		batch.t.lock.RLock()
		defer batch.t.lock.RUnlock()
	} else {
		batch.t.lock.Lock()
		defer batch.t.lock.Unlock()
	}
	if batch.t.index == nil || batch.t.head == nil {
		return false, errClosed
	}
	// Ensure we're in sync with the data
	if atomic.LoadUint64(&batch.t.items) != batch.firstIdx {
		return false, fmt.Errorf("appending unexpected item: want %d, have %d", batch.t.items, batch.firstIdx)
	}
	if newHead {
		if err := batch.t.advanceHead(); err != nil {
			return false, err
		}
		// And update the batch to point to the new file
		batch.headBytes = 0
	}
	var (
		filenum         = atomic.LoadUint32(&batch.t.headId)
		indexData       = make([]byte, 0, len(batch.sizes)*indexEntrySize)
		count           uint64
		writtenDataSize int
	)
	for _, size := range batch.sizes {
		if batch.headBytes+size <= batch.t.maxFileSize {
			writtenDataSize += int(size)
			idx := indexEntry{
				filenum: filenum,
				offset:  batch.headBytes + size,
			}
			batch.headBytes += size
			idxData := idx.marshallBinary()
			indexData = append(indexData, idxData...)
		} else {
			// Writing will overflow, need to chunk up the batch into several writes
			break
		}
		count++
	}
	if writtenDataSize == 0 {
		return batch.count > 0, nil
	}
	// Write the actual data
	if _, err := batch.t.head.Write(batch.buf.Next(writtenDataSize)); err != nil {
		return false, err
	}
	// Write the new indexdata
	if _, err := batch.t.index.Write(indexData); err != nil {
		return false, err
	}
	batch.t.writeMeter.Mark(int64(batch.buf.Len()) + int64(batch.count)*int64(indexEntrySize))
	batch.t.sizeGauge.Inc(int64(batch.buf.Len()) + int64(batch.count)*int64(indexEntrySize))
	atomic.AddUint64(&batch.t.items, count)
	batch.firstIdx += count
	batch.count -= count

	if batch.count > 0 {
		// Some data left to write on a retry.
		batch.sizes = batch.sizes[count:]
		return true, nil
	}
	// All data written. We can simply truncate and keep using the buffer
	batch.sizes = batch.sizes[:0]
	return false, nil
}

func (batch *freezerTableBatch) Size() int {
	return batch.buf.Len()
}

// freezerBatch is a write-only database that commits changes to the associated
// ancient store when Write is called. A freezerBatch cannot be used concurrently.
type freezerBatch struct {
	f       *freezer
	batches map[string]*freezerTableBatch // List of table batches
	size    int
}

// AppendAncient injects all binary blobs belong to block into the batch.
func (batch *freezerBatch) AppendAncient(number uint64, hash, header, body, receipt, td []byte) error {
	if err := batch.batches[freezerHeaderTable].Append(number, header); err != nil {
		return err
	}
	if err := batch.batches[freezerHashTable].Append(number, hash); err != nil {
		return err
	}
	if err := batch.batches[freezerBodiesTable].Append(number, body); err != nil {
		return err
	}
	if err := batch.batches[freezerReceiptTable].Append(number, receipt); err != nil {
		return err
	}
	if err := batch.batches[freezerDifficultyTable].Append(number, td); err != nil {
		return err
	}
	return nil
}

// TruncateAncients discards all but the first n ancient data from the ancient store.
func (batch *freezerBatch) TruncateAncients(n uint64) error {
	return errNotSupported
}

// Sync flushes all in-memory ancient store data to disk.
func (batch *freezerBatch) Sync() error {
	return errNotSupported
}

// valueSize retrieves the amount of data queued up for writing.
// It's the internal version without holding the lock.
func (batch *freezerBatch) ValueSize() int {
	var size int
	for _, batch := range batch.batches {
		size += batch.Size()
	}
	return size
}

// Write flushes any accumulated data to ancient store.
func (batch *freezerBatch) Write(maxReserved int) error {
	if batch.f.readonly {
		return errReadOnly
	}
	// The flush order is a bit special here. Considering the large difference
	// in data size of different data types in freezer, the bigger data is
	// flushed first to that the tiny data can be kept in the batch in order to
	// save syscalls.
	total := batch.ValueSize()
	for _, tableBatch := range []*freezerTableBatch{batch.batches[freezerReceiptTable], batch.batches[freezerBodiesTable],
		batch.batches[freezerHeaderTable], batch.batches[freezerHashTable], batch.batches[freezerDifficultyTable]} {
		if total < maxReserved {
			return nil
		}
		size := tableBatch.Size()
		if err := tableBatch.Write(); err != nil {
			return err
		}
		total -= size
	}
	return nil
}
