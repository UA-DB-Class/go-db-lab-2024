package godb

import (
	"bufio"
	//<silentstrip lab1>
	"bytes"
	//</silentstrip>
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

// HeapFile is an unordered collection of tuples Internally, it is arranged as a
// set of heapPage objects
//
// HeapFile is a public class because external callers may wish to instantiate
// database tables using the method [LoadFromCSV]
type HeapFile struct {
	//<strip lab1>
	td            *TupleDesc
	numPages      int
	backingFile   string
	lastEmptyPage int
	//</strip>
	// HeapFile should include the fields below;  you may want to add
	// additional fields
	bufPool *BufferPool
	sync.Mutex
}

// <silentstrip lab1>
type heapFileRid struct {
	pageNo int
	slotNo int
}

//</silentstrip>

// Create a HeapFile.
// Parameters
// - fromFile: backing file for the HeapFile.  May be empty or a previously created heap file.
// - td: the TupleDesc for the HeapFile.
// - bp: the BufferPool that is used to store pages read from the HeapFile
// May return an error if the file cannot be opened or created.
func NewHeapFile(fromFile string, td *TupleDesc, bp *BufferPool) (*HeapFile, error) {
	//<strip lab1>
	f, err := os.OpenFile(fromFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	numPages := fi.Size() / int64(PageSize)
	return &HeapFile{td, int(numPages), fromFile, -1, bp, sync.Mutex{}}, nil
	//</strip>
	//<insert lab1>
	//return &HeapFile{}, nil //replace me
	//</insert>
}

// Return the number of pages in the heap file
func (f *HeapFile) NumPages() int {
	//<strip lab1>
	return f.numPages
	//</strip>
	//<insert lab1>
	//return 0 //replace me
	//</insert>
}

// Load the contents of a heap file from a specified CSV file.  Parameters are as follows:
// - hasHeader:  whether or not the CSV file has a header
// - sep: the character to use to separate fields
// - skipLastField: if true, the final field is skipped (some TPC datasets include a trailing separator on each line)
// Returns an error if the field cannot be opened or if a line is malformed
// We provide the implementation of this method, but it won't work until
// [HeapFile.insertTuple] is implemented
func (f *HeapFile) LoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
	scanner := bufio.NewScanner(file)
	cnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, sep)
		if skipLastField {
			fields = fields[0 : len(fields)-1]
		}
		numFields := len(fields)
		cnt++
		desc := f.Descriptor()
		if desc == nil || desc.Fields == nil {
			return GoDBError{MalformedDataError, "Descriptor was nil"}
		}
		if numFields != len(desc.Fields) {
			return GoDBError{MalformedDataError, fmt.Sprintf("LoadFromCSV:  line %d (%s) does not have expected number of fields (expected %d, got %d)", cnt, line, len(f.Descriptor().Fields), numFields)}
		}
		if cnt == 1 && hasHeader {
			continue
		}
		var newFields []DBValue
		for fno, field := range fields {
			switch f.Descriptor().Fields[fno].Ftype {
			case IntType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				intValue := int(floatVal)
				newFields = append(newFields, IntField{int64(intValue)})
			case StringType:
				if len(field) > StringLength {
					field = field[0:StringLength]
				}
				newFields = append(newFields, StringField{field})
			}
		}
		newT := Tuple{*f.Descriptor(), newFields, nil}
		tid := NewTID()
		bp := f.bufPool
		bp.BeginTransaction(tid)
		f.insertTuple(&newT, tid)

		// hack to force dirty pages to disk
		// because CommitTransaction may not be implemented
		// yet if this is called in lab 1 or 2
		for j := 0; j < f.NumPages(); j++ {
			pg, err := bp.GetPage(f, j, tid, ReadPerm)
			if pg == nil || err != nil {
				fmt.Println("page nil or error", err)
				break
			}
			if (*pg).isDirty() {
				(*f).flushPage(pg)
				(*pg).setDirty(false)
			}

		}

		//commit frequently, to avoid all pages in BP being full
		//todo fix
		bp.CommitTransaction(tid)
	}
	return nil
}

// Read the specified page number from the HeapFile on disk.  This method is
// called by the [BufferPool.GetPage] method when it cannot find the page in its
// cache.
//
// This method will need to open the file supplied to the constructor, seek to the
// appropriate offset, read the bytes in, and construct a [heapPage] object, using
// the [heapPage.initFromBuffer] method.
func (f *HeapFile) readPage(pageNo int) (*Page, error) {
	//<strip lab1>
	file, err := os.OpenFile(f.backingFile, os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	b := make([]byte, PageSize)
	n, err := file.ReadAt(b, int64(pageNo*PageSize))
	if err != nil {
		return nil, err
	}
	if n != PageSize {
		return nil, GoDBError{MalformedDataError, "not enough bytes read in ReadPage"}
	}
	buf := bytes.NewBuffer(b)
	pg := newHeapPage(f.Descriptor(), pageNo, f)
	pg.initFromBuffer(buf)
	var p Page = pg
	return &p, nil
	//</strip>
	//<insert lab1>
	//return nil, nil
	//</insert>
}

// Add the tuple to the HeapFile.  This method should search through pages in
// the heap file, looking for empty slots and adding the tuple in the first
// empty slot if finds.
//
// If none are found, it should create a new [heapPage] and insert the tuple
// there, and write the heapPage to the end of the HeapFile (e.g., using the
// [flushPage] method.)
//
// To iterate through pages, it should use the [BufferPool.GetPage method]
// rather than directly reading pages itself. For lab 1, you do not need to
// worry about concurrent transactions modifying the Page or HeapFile.  We will
// add support for concurrent modifications in lab 3.
//
// The page the tuple is inserted into should be marked as dirty.
func (f *HeapFile) insertTuple(t *Tuple, tid TransactionID) error {
	//<strip lab1>
	var start int
	f.Lock()
	if f.lastEmptyPage == -1 {
		start = 0
	} else {
		start = f.lastEmptyPage
	}
	endPage := f.numPages
	f.Unlock()
	for p := start; p < endPage; p++ {
		pg, err := f.bufPool.GetPage(f, p, tid, ReadPerm)
		if err != nil {
			return err
		}
		if (*pg).(*heapPage).getNumEmptySlots() == 0 {
			continue
		}

		pg, err = f.bufPool.GetPage(f, p, tid, WritePerm)
		if err != nil {
			return err
		}
		heapp := (*pg).(*heapPage)
		_, err = heapp.insertTuple(t)
		if err == nil {
			heapp.setDirty(true)
			f.lastEmptyPage = p // this is fine because lastEmptyPage is a hint, not forcing
			return nil
		}
	}
	f.Lock()
	//no free slots, create new page
	heapp := newHeapPage(f.td, f.numPages, f)
	var pg Page = heapp
	err := f.flushPage(&pg) // flush an empty page to later add to buffer pool, helps maintain dirtiness
	f.lastEmptyPage = f.numPages
	p := f.numPages
	f.numPages++
	f.Unlock()

	if err != nil {
		return err
	} else {
		pg, err := f.bufPool.GetPage(f, p, tid, WritePerm)
		if err != nil {
			return err
		}
		heapp := (*pg).(*heapPage)
		_, err = heapp.insertTuple(t)
		if err == nil {
			heapp.setDirty(true)
			f.lastEmptyPage = p
			return nil
		} else {
			return err // must success because its the new page
		}
	}

	//</strip>
	// <insert lab1>
	// return nil //replace me
	// </insert>

}

// Remove the provided tuple from the HeapFile.  This method should use the
// [Tuple.Rid] field of t to determine which tuple to remove.
// This method is only called with tuples that are read from storage via the
// [Iterator] method, so you can so you can supply the value of the Rid
// for tuples as they are read via [Iterator].  Note that Rid is an empty interface,
// so you can supply any object you wish.  You will likely want to identify the
// heap page and slot within the page that the tuple came from.
//
// The page the tuple is deleted from should be marked as dirty.

func (f *HeapFile) deleteTuple(t *Tuple, tid TransactionID) error {
	//<strip lab1>
	var rid heapFileRid
	var ok bool
	precheck := func() error {
		f.Lock()
		defer f.Unlock()

		if t.Rid == nil {
			return GoDBError{TupleNotFoundError, "provided tuple has null rid, cannot delete"}
		}

		rid, ok = t.Rid.(heapFileRid)

		if !ok {
			return GoDBError{TupleNotFoundError, "provided tuple is not a heap file tuple, based on rid"}
		}
		if rid.pageNo < 0 || rid.pageNo >= f.numPages {
			return GoDBError{TupleNotFoundError, "provided tuple references a page that does not exists"}
		}
		return nil
	}
	if err := precheck(); err != nil {
		return err
	}
	pg, err := f.bufPool.GetPage(f, rid.pageNo, tid, WritePerm)
	if err != nil {
		return err
	}
	heapp, ok := (*pg).(*heapPage)
	if !ok {
		return GoDBError{IncompatibleTypesError, "buffer pool returned non-heap page when heap page expected"}
	}
	heapp.setDirty(true)
	err = heapp.deleteTuple(rid)
	if err == nil {
		f.Lock()
		if rid.pageNo < f.lastEmptyPage {
			f.lastEmptyPage = rid.pageNo
		}
		f.Unlock()
	}
	return err
	//</strip>
	// <insert lab1>
	// return nil //replace me
	// </insert>
}

// Method to force the specified page back to the backing file at the appropriate
// location.  This will be called by BufferPool when it wants to evict a page.
// The Page object should store information about its offset on disk (e.g.,
// that it is the ith page in the heap file), so you can determine where to write it
// back.
func (f *HeapFile) flushPage(p *Page) error {
	//<strip lab1>
	// note that this method is not thread safe
	file, err := os.OpenFile(f.backingFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	hp := (*p).(*heapPage)

	buf, err := hp.toBuffer()
	if err != nil {
		return err
	}
	_, err = file.WriteAt(buf.Bytes(), int64(hp.pageNo*PageSize))
	return err
	// </strip>
	// <insert lab1>
	// return nil //replace me
	// </insert>
}

// [Operator] descriptor method -- return the TupleDesc for this HeapFile
// Supplied as argument to NewHeapFile.
func (f *HeapFile) Descriptor() *TupleDesc {
	//<strip lab1>
	return f.td
	//</strip>
	// <insert lab1>
	// return nil //replace me
	// </insert>

}

// [Operator] iterator method
// Return a function that iterates through the records in the heap file
// Note that this method should read pages from the HeapFile using the
// BufferPool method GetPage, rather than reading pages directly,
// since the BufferPool caches pages and manages page-level locking state for
// transactions
// You should esnure that Tuples returned by this method have their Rid object
// set appropriate so that [deleteTuple] will work (see additional comments there).
func (f *HeapFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {

	//<strip lab1>
	pgNo := 0
	var pgIter func() (*Tuple, error)

	return func() (*Tuple, error) {
		var next *Tuple
		for {
			if pgIter != nil {
				next, err := pgIter()
				if err != nil {
					return nil, err
				}
				if next != nil {
					return &Tuple{*f.td, next.Fields, next.Rid}, nil
				}
			}
			if pgIter == nil || next == nil {
				if pgNo == f.numPages {
					return nil, nil
				}
				p, err := f.bufPool.GetPage(f, pgNo, tid, ReadPerm)
				if err != nil {
					return nil, err
				}
				heapp := (*p).(*heapPage) //assume this is a heapPage object
				pgIter = heapp.tupleIter()
				pgNo = pgNo + 1
			}
		}
	}, nil
	//</strip>
	//<insert lab1>
	//return func() (*Tuple, error) {
	//	return nil, nil
	//}, nil
	//</insert>

}

// internal strucuture to use as key for a heap page
type heapHash struct {
	FileName string
	PageNo   int
}

// This method returns a key for a page to use in a map object, used by
// BufferPool to determine if a page is cached or not.  We recommend using a
// heapHash struct as the key for a page, although you can use any struct that
// does not contain a slice or a map that uniquely identifies the page.
func (f *HeapFile) pageKey(pgNo int) any {

	//<strip lab1>
	var hh heapHash = heapHash{f.backingFile, pgNo}
	return hh
	//</strip>
	//<insert lab1>
	//return nil
	//</insert>

}
