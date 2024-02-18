package godb

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

import (
	//<silentstrip lab1>

	"sync"
	"time"
	//</silentstrip>
)

// Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

type BufferPool struct {
	//<strip lab1|lab2|lab3>
	pages       map[any]*Page
	readLocks   map[any][]TransactionID
	writeLocks  map[any]TransactionID
	pageConds   map[any]*sync.Cond
	tidPageList map[TransactionID][]any
	runningTids map[TransactionID]any
	waitGraph   waitsFor
	maxPages    int
	sync.Mutex
	//</strip>
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) *BufferPool {
	//<strip lab1|lab2|lab3>
	return &BufferPool{make(map[any]*Page), make(map[any][]TransactionID),
		make(map[any]TransactionID), make(map[any]*sync.Cond),
		make(map[TransactionID][]any), make(map[TransactionID]any), waitsFor{}, numPages, sync.Mutex{}}
	//</strip>
	//<insert lab1>
	//return &BufferPool{}
	//</insert>
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe
func (bp *BufferPool) FlushAllPages() {
	//<strip lab1|lab2|lab3>
	for _, page := range bp.pages {
		file := (*page).getFile()
		(*file).flushPage(page)
	}
	//</strip>
}

// <silentstrip lab1|lab2|lab3>
func (bp *BufferPool) releaseLocks(tid TransactionID) {
	pgList := bp.tidPageList[tid]
	for _, pg := range pgList {
		got := false
		readLocks := bp.readLocks[pg]
		for i, testTid := range readLocks {
			if testTid == tid {
				readLocks[i] = readLocks[len(readLocks)-1]
				bp.readLocks[pg] = readLocks[:len(readLocks)-1]
				got = true
			}
		}
		writeLocks := bp.writeLocks[pg]

		if writeLocks == tid {
			delete(bp.writeLocks, pg)
			got = true
		}
		if got {
			cond := bp.pageConds[pg]
			cond.Signal()
		}
	}
	delete(bp.tidPageList, tid)
	bp.waitGraph.removeTransaction(tid)
}

//</silentstrip>

// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtired will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	//<strip lab1|lab2|lab3>
	bp.Lock()
	defer bp.Unlock()
	if !bp.tidIsRunning(tid) {
		//todo return error
		return
	}
	delete(bp.runningTids, tid)
	pgList := bp.tidPageList[tid]
	for _, pg := range pgList {
		writeLocks := bp.writeLocks[pg]

		if writeLocks == tid {
			delete(bp.pages, pg)
		}
	}
	bp.releaseLocks(tid)
	// </strip>
}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	//<strip lab1|lab2|lab3>
	bp.Lock()
	defer bp.Unlock()
	if !bp.tidIsRunning(tid) {
		//todo return error
		return
	}
	delete(bp.runningTids, tid)

	pgList := bp.tidPageList[tid]
	for _, pg := range pgList {
		writeLocks := bp.writeLocks[pg]

		if writeLocks == tid {
			page := bp.pages[pg]
			if page == nil { //page write locked but not dirtied
				continue
			}
			if (*page).isDirty() {
				(*(*page).getFile()).flushPage(page)
				(*page).setDirty(false)
			}
		}
	}

	bp.releaseLocks(tid)

	// </strip>
}

// <silentstrip lab1|lab2|lab3}>
func (bp *BufferPool) tidIsRunning(tid TransactionID) bool {
	hasTid := bp.runningTids[tid]
	return hasTid != nil
}

//</silentstrip>

func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	//<strip lab1|lab2|lab3>
	bp.Lock()
	defer bp.Unlock()
	if bp.tidIsRunning(tid) {
		return GoDBError{IllegalTransactionError, "transaction already running"}
	}
	bp.runningTids[tid] = true
	//</strip>
	return nil
}

// <silentstrip lab1|lab2|lab3>
func (bp *BufferPool) evictPage() error {
	if len(bp.pages) >= bp.maxPages {
		var evictKey any
		//evict first non-dirty page
		//todo fix dirty logic - mark modified pages as dirty and
		//remove this check until transactions are added
		for key, page := range bp.pages {
			if !(*page).isDirty() {
				evictKey = key
				break
			}
		}
		if evictKey == nil {
			//bp.Unlock()
			return GoDBError{BufferPoolFullError, "all pages in buffer pool are dirty"}
		}
		//note locks retained across eviction
		delete(bp.pages, evictKey)

	}
	return nil

}

type waitsFor map[TransactionID][]TransactionID

func (w waitsFor) addEdge(tid TransactionID, tids []TransactionID) {
	edges := w[tid]
	for _, t := range tids {
		if t == nil {
			continue
		}
		found := false
		for _, e := range edges {
			if e == t {
				found = true
				break
			}
		}
		if !found {
			edges = append(edges, t)
		}
	}
	w[tid] = edges
}

func (w waitsFor) removeTransaction(tid TransactionID) {
	delete(w, tid)
	for t := range w {
		if t == nil {
			continue
		}
		var newWaitTid []TransactionID
		for _, tt := range w[t] {
			if tt != tid {
				newWaitTid = append(newWaitTid, tt)
			}
		}
		w[t] = newWaitTid
	}
}

func breakCycle(w waitsFor, start TransactionID, root TransactionID, seen map[TransactionID]bool) TransactionID {
	nodes := w[start]
	for _, n := range nodes {
		if n == root { // found cycle
			return n
		}
		if _, exists := seen[n]; !exists {
			seen[n] = true
			deadlockTID := breakCycle(w, n, root, seen)
			if deadlockTID != nil {
				return deadlockTID
			}
		}
	}
	return nil
}

func deadlockDetect(w waitsFor, start TransactionID, seen []TransactionID) TransactionID {
	seenSet := make(map[TransactionID]bool)
	for _, n := range seen {
		seenSet[n] = true
	}
	return breakCycle(w, start, start, seenSet)
}

func (bp *BufferPool) checkDeadlock(tid TransactionID) TransactionID {
	return deadlockDetect(bp.waitGraph, tid, []TransactionID{tid})
}

//</silentstrip>

// Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk uing [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pages), a page should be evicted.  Should not evict
// pages that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pages, you should return an error. Before returning the page,
// attempt to lock it with the specified permission.  If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock. For lab 1, you do not need to
// implement locking or deadlock detection. You will likely want to store a list
// of pages in the BufferPool in a map keyed by the [DBFile.pageKey].
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (*Page, error) {
	//<strip lab1|lab2|lab3>
	bp.Lock()
	if !bp.tidIsRunning(tid) {
		bp.Unlock()
		return nil, GoDBError{IllegalTransactionError, "Transaction is not running or has aborted."}
	}
	bp.Unlock()
	hashCode := file.pageKey(pageNo)
	var pg *Page
	var err error
	success := false
	for { //loop until locks are acquired
		bp.Lock()
		pg = bp.pages[hashCode]
		if pg == nil {
			pg, err = file.readPage(pageNo)
			if err != nil {
				bp.Unlock()
				return nil, err
			}
			err = bp.evictPage()
			if err != nil {
				bp.Unlock()
				return nil, err
			}
			bp.pages[hashCode] = pg
			if bp.readLocks[hashCode] == nil {
				bp.readLocks[hashCode] = make([]TransactionID, 0)
				mutex := sync.Mutex{}
				bp.pageConds[hashCode] = sync.NewCond(&mutex)
			}
		}
		switch perm {
		case ReadPerm:
			//check if page has write lock
			writeTid := bp.writeLocks[hashCode]
			if writeTid == nil || writeTid == tid {
				//add read locks
				readList := bp.readLocks[hashCode]
				found := false
				for _, checkTid := range readList {
					if checkTid == tid {
						found = true
					}
				}
				if !found {
					readList = append(readList, tid)
					bp.readLocks[hashCode] = readList
					pgList := bp.tidPageList[tid]
					pgList = append(pgList, hashCode)
					bp.tidPageList[tid] = pgList
				}
				success = true
			} else {
				bp.waitGraph.addEdge(tid, []TransactionID{writeTid})
			}
		case WritePerm:
			writeTid := bp.writeLocks[hashCode]
			readTids := bp.readLocks[hashCode]
			readOk := len(readTids) == 0 || (len(readTids) == 1 && readTids[0] == tid)
			writeOk := writeTid == nil || writeTid == tid

			if readOk && writeOk {
				//add write
				if writeTid != tid {
					bp.writeLocks[hashCode] = tid
					pgList := bp.tidPageList[tid]
					pgList = append(pgList, hashCode)
					bp.tidPageList[tid] = pgList
				}
				success = true
			} else {
				bp.waitGraph.addEdge(tid, []TransactionID{writeTid})
				var readWaits []TransactionID
				for _, t := range readTids {
					if t != tid {
						readWaits = append(readWaits, t)
					}
				}
				bp.waitGraph.addEdge(tid, readWaits)
			}

		}
		if success {
			bp.Unlock()
			break
		}
		deadlockTid := bp.checkDeadlock(tid)
		//fmt.Println(*tid, "(", tid, ")", "waitgraph: ", bp.waitGraph)
		bp.Unlock()
		if deadlockTid != nil {
			bp.AbortTransaction(deadlockTid)
			//fmt.Println(*tid, "(", tid, ")", " aborted ", *deadlockTid, "(", deadlockTid, ")")
			if *deadlockTid == *tid {
				// fmt.Println("suicide", *deadlockTid)
				//fmt.Println("return nil")
				time.Sleep(10 * time.Millisecond)
				return nil, GoDBError{IllegalTransactionError, "Transaction has suicided."}
			}
		}
		time.Sleep(2 * time.Millisecond)
	}

	return pg, nil
	// </strip>
	// <insert lab1|lab2|lab3>
	// return nil, nil
	// </insert>
}
