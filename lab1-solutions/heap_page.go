package godb

import (
	"bytes"
	//<silentstrip lab1>
	"encoding/binary"
	"unsafe"
	//</silentstrip>
)

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type heapPage struct {
	//<strip lab1>
	desc     TupleDesc
	numSlots int32
	numUsed  int32
	dirty    bool
	tuples   []*Tuple
	pageNo   int
	file     *HeapFile
	//</strip>
}

// <silentstrip lab1>
func headerSizeInBytesAndNumSlots(desc *TupleDesc) (int32, int32) {
	headerSize := unsafe.Sizeof(int32(0)) * 2
	remSize := PageSize - int(headerSize)
	numSlots := (int)(float32(remSize) / (float32(desc.bytesPerTuple())))
	return int32(headerSize), int32(numSlots)
}

//</silentstrip>

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) *heapPage {
	//<strip lab1>
	_, numSlots := headerSizeInBytesAndNumSlots(desc)
	h := heapPage{*desc, numSlots, 0, false, make([]*Tuple, numSlots), pageNo, f}
	return &h
	//</strip>
	//<insert lab1>
	//return &heapPage{} //replace me
	//</insert>
}

func (h *heapPage) getNumSlots() int {
	//<strip lab1>
	return int(h.numSlots)
	//</strip>
	//<insert lab1>
	//return 0 //replace me
	//</insert>
}

// <silentstrip lab1>
func (h *heapPage) getNumEmptySlots() int {
	numEmpty := 0
	for i := 0; i < int(h.numSlots); i++ {
		if h.tuples[i] == nil {
			numEmpty++
		}
	}
	return numEmpty
}

//</silentstrip>

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	//<strip lab1>
	for i := 0; i < int(h.numSlots); i++ {
		if h.tuples[i] == nil {
			h.tuples[i] = t
			h.numUsed++
			t.Rid = heapFileRid{h.pageNo, i}
			return t.Rid, nil
		}
	}
	return 0, GoDBError{PageFullError, "no space on page to insert"}
	//</strip>
	//<insert lab1>
	//return 0, nil //replace me
	//</insert>
}

// Delete the tuple in the specified slot number, or return an error if
// the slot is invalid
func (h *heapPage) deleteTuple(rid recordID) error {
	//<strip lab1>
	heapRid, ok := rid.(heapFileRid)
	if !ok {
		return GoDBError{TupleNotFoundError, "supplied rid is not a heapFileRid"}
	}
	slot := heapRid.slotNo
	if slot < 0 || slot >= int(h.numSlots) {
		return GoDBError{TupleNotFoundError, "slot does not exist on delete"}
	}
	//todo check if slot is non-null?
	if h.tuples[slot] == nil {
		return GoDBError{TupleNotFoundError, "element already deleted"}
	}
	h.numUsed--
	h.tuples[slot] = nil
	return nil
	//</strip>
	//<insert lab1>
	//return nil //replace me
	//</insert>

}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	//<strip lab1>
	return h.dirty
	//</strip>
	//<insert lab1>
	//return false //replace me
	//</insert>
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(dirty bool) {
	//<strip lab1>
	h.dirty = dirty
	//</strip>
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() *DBFile {
	//<strip lab1>
	var f DBFile = p.file
	return &f
	//</strip>
	//<insert lab1>
	//return nil //replace me
	//</insert>
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	//<strip lab1>
	b := new(bytes.Buffer)
	var err error = nil

	err = binary.Write(b, binary.LittleEndian, (int32)(h.numSlots))
	if err != nil {
		return nil, err
	}
	err = binary.Write(b, binary.LittleEndian, (int32)(h.numUsed))
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(h.tuples); i++ {

		t := h.tuples[i]
		if t != nil {
			err = t.writeTo(b)
			if err != nil {
				return nil, err
			}
		}
	}
	if b.Len() > PageSize {
		return nil, GoDBError{MalformedDataError, "buffer is greater than page size"}
	}
	arSize := PageSize - b.Len()
	ar := make([]byte, arSize)
	b.Write(ar)

	return b, nil
	//</strip>
	//<insert lab1>
	//return nil,nil //replace me
	//</insert>

}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	//<strip lab1>
	var numSlotsHeader, numUsedHeader int32
	err := binary.Read(buf, binary.LittleEndian, &numSlotsHeader)
	if err != nil {
		return err

	}
	err = binary.Read(buf, binary.LittleEndian, &numUsedHeader)
	if err != nil {
		return err

	}
	tups := make([]*Tuple, numSlotsHeader)
	for i := 0; i < int(numUsedHeader); i++ {
		t, err := readTupleFrom(buf, &h.desc)
		t.Rid = heapFileRid{h.pageNo, i}
		if err != nil {
			return err
		}
		tups[i] = t
	}
	h.numSlots = numSlotsHeader
	h.numUsed = numUsedHeader
	h.dirty = false
	h.tuples = tups
	return nil
	//</strip>
	//<insert lab1>
	//return nil //replace me
	//</insert>
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (p *heapPage) tupleIter() func() (*Tuple, error) {
	//<strip lab1>
	i := 0
	return func() (*Tuple, error) {
		for j := i; j < len(p.tuples); j++ {
			t := p.tuples[i]
			i = j + 1
			if t != nil {
				return t, nil
			}
		}
		return nil, nil
	}
	//</strip>
	//<insert lab1>
	//return nil //replace me
	//</insert>
}
