package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 100;
    
    /**
     * Map that stores pages in this BufferPool
     * Maps PageId -> Page
     */
    private Map<PageId, Page> BPoolPageMap;
    
    /**
     * capacity of the BufferPool
     */
    private int capacity;
    
    /**
     * manages locking of pages in the Bufferpool
     */
    private LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        capacity = numPages;
        BPoolPageMap = new HashMap<PageId, Page>();
        lockManager = new LockManager();
    }
    
    public int getPageSize()
    {
    	return 4096;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
    	
    	// check for deadlock by a simple timeout policy
    	
    	// initial lock request time
    	long t0 = System.currentTimeMillis();    	    	
    	// try to get a lock. If lock denied, sleep and try again.
    	boolean lockIssued = lockManager.setLock(tid, pid, perm);
    	
    	while (!lockIssued) {
    		
    		// next lock requestime
    		long t1 = System.currentTimeMillis();
    		if((t1 - t0) > 500){
    			// assume deadlock
    			throw new TransactionAbortedException();
    		}
    		try {
				Thread.sleep(10);
				lockIssued = lockManager.setLock(tid, pid, perm);
			} catch (InterruptedException e) {
				
				e.printStackTrace();
			}
    	}
    	
        if(BPoolPageMap.containsKey(pid)){
        	//page found in BufferPool
        	return BPoolPageMap.get(pid);
        } else {
        	//page not found in BufferPool. Get from disk
        	
        	// get HeapFiles from each table
        	List<Table> tableList = Database.getCatalog().getTables();
        	
        	// for each table, compare the id of it's DbFile to the table id
        	// contained in the given pid
        	for(Table table : tableList){
        		// table containing required page found
        		if(table.getFile().getId() == pid.getTableId()){
        			Page pageFromDisk = table.getFile().readPage(pid);
        			
        			// if BufferPool full, throw DbException
        			// remove this later
        			if(capacity == BPoolPageMap.size()){
        				//throw new DbException("BufferPool capacity reached");
        				evictPage();
        			}	
        			BPoolPageMap.put(pid, pageFromDisk);
        			        			
        			return pageFromDisk;
        		}
        		
        	}
        	// page not found on in BufferPool or on disk
        	throw new DbException("Requested page not found in database");
        }
		
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
       if(commit){
    	   // flush dirty pages associated to the transaction to disk
    	   // flushPages(tid);
    	 //flush dirty pages associated to the transaction to disk
	       	Collection<Page> allPages = BPoolPageMap.values();
	       	for(Page p : allPages){
	       		if(p.isDirty() != null && p.isDirty().equals(tid)){
	       			
	       			flushPage(p.getId());
	       		    // use current page contents as the before-image
	       	        // for the next transaction that modifies this page.
	       	        p.setBeforeImage();
	       			
	       		}
	       		if(p.isDirty() == null){
	       			p.setBeforeImage();
	       			
	       		}
	       	}
        	       	
    	   lockManager.releaseAllLocks(tid);
       } else {
    	   //aborted
    	   //revert any changes made by the transaction by restoring the page to its on-disk state. 
    	   Collection<Page> p = BPoolPageMap.values();
    	   for(Page page : p){
    		   if(page.isDirty() != null && page.isDirty().equals(tid)){
    			   PageId pid = page.getId();
    			   //page.setBeforeImage();
    			   Page oldPage = page.getBeforeImage();
    			   BPoolPageMap.put(pid, oldPage);
    			   
    		   }
    	   }
    	   lockManager.releaseAllLocks(tid);
       }
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
    	
    	HeapFile f = (HeapFile)Database.getCatalog().getDbFile(tableId);
    	ArrayList<Page> modifiedPages = f.insertTuple(tid, t);
    	Page modifiedPage = modifiedPages.get(0);
    	
    	//markpage dirty and update it in the buffer pool
    	modifiedPage.markDirty(true, tid);
    	BPoolPageMap.put(modifiedPage.getId(), modifiedPage);
    	
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
    	int tableId=  t.getRecordId().getPageId().getTableId();
    	HeapFile f = (HeapFile)Database.getCatalog().getDbFile(tableId);
    	Page modifiedPage = f.deleteTuple(tid, t);
       	
    	//mark page dirty and update it in the buffer pool
    	modifiedPage.markDirty(true, tid);
    	
    	
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // calls flushPage on all the pages in bufferpool
    	Set<PageId> pageIDs = BPoolPageMap.keySet();
    	for(PageId pid : pageIDs){
    		flushPage(pid);
    	}

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        BPoolPageMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        Page pageToFlush = BPoolPageMap.get(pid);
        if(pageToFlush.isDirty() != null){
        	
        	// append an update record to the log, with 
            // a before-image and after-image.
            TransactionId dirtier = pageToFlush.isDirty();
            if (dirtier != null){
              Database.getLogFile().logWrite(dirtier, pageToFlush.getBeforeImage(), pageToFlush);
              Database.getLogFile().force();
            }
            
        	// page is dirty
        	Database.getCatalog().getDbFile(pid.getTableId()).writePage(pageToFlush);
        } else {
        	// page not dirty
        	// do nothing
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
    	//flush dirty pages associated to the transaction to disk
    	Collection<Page> allPages = BPoolPageMap.values();
    	for(Page p : allPages){
    		if(p.isDirty() != null && p.isDirty().equals(tid)){
    			
    			flushPage(p.getId());
    			
    		}
    	}
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
       Set<PageId> bufferpoolPIDs = BPoolPageMap.keySet();
       List<PageId> pidList = new ArrayList<PageId>(bufferpoolPIDs);
       
       boolean evicted = false;
	    // check if all the pages are dirty
	   	boolean allDirty = true;
	   	int pageCount = 0;
	   	Collection<Page> allPages = BPoolPageMap.values();
	   	for(Page p: allPages){
	   		if(p.isDirty() == null){
	   			// at least one non dirty page exists
	   			allDirty = false;
	   			break;
	   		} else {
	   			pageCount++;
	   		}
	   	}
	   	
	   	if(allDirty && pageCount == capacity){
	   		throw new DbException("all pages in BufferPool are dirty. Cannot evict.");
	   	}
       
       while(!evicted){
	       // pick a random page ID from pages in bufferPool to evict
	       Random randomGenerator = new Random();
	       int randomInt = randomGenerator.nextInt(pidList.size());
	       
	       PageId pageIdToEvict = pidList.get(randomInt);
	       Page pageToChk = BPoolPageMap.get(pageIdToEvict);
	       
	       // check if a transaction has written data to the page
	       // for NO STEAL
	       if(pageToChk.isDirty() == null){
	    	   try {
				   flushPage(pageIdToEvict);
				   BPoolPageMap.remove(pageIdToEvict);
				   evicted = true;
			   } catch (IOException e) {
				   System.out.println(e.getMessage());
				   e.printStackTrace();
			   }
	    	   
	       }
	   }
    }
    
    private class LockManager {
    	
    	/**
    	 * Keeps track of the transactions that hold a shared lock on a page 
    	 * Maps PageId -> Set of transaction ids
    	 */
    	private Map<PageId, Set<TransactionId>> sharedLocks; 
    	
    	/**
    	 * Keeps track of the transactions that hold an exclusive lock on a page
    	 * Maps PageId -> Set of transaction ids
    	 */
    	private Map<PageId, TransactionId> exclusiveLocks;
    	
    	/**
    	 * Keeps track of the pages that a transaction has a shared lock on
    	 * Used to release all shared locks by a transaction
    	 */
    	private Map<TransactionId, Set<PageId>> sharedLockedPages;
    	
    	/**Keeps track of the page that a transaction has an exclusive lock on
    	 * USed to release all exclusive locks by a transaction
    	 * 
    	 */
    	private Map<TransactionId, Set<PageId>> exclusiveLockedPages;
    	
    	public LockManager(){
    		sharedLocks = new HashMap<PageId, Set<TransactionId>>();
    		exclusiveLocks = new HashMap<PageId, TransactionId>();
    		sharedLockedPages = new HashMap<TransactionId, Set<PageId>>();
    		exclusiveLockedPages = new HashMap<TransactionId, Set<PageId>>();
    	}
    	
    	/**
    	 * Sets the lock requested by the transaction. If lock cannot be granted, puts the transaction on 
    	 * a wait list
    	 * @param tid id of the requesting a lock
    	 * @param pid id of page to be locked
    	 * @param perm permission (read -> shared lock, read_write -> exclusive lock)
    	 * @return true if the lock was granted, false if lock not granted
    	 */
    	public synchronized boolean setLock(TransactionId tid, PageId pid, Permissions perm){
    		if(perm.equals(Permissions.READ_ONLY)){
    			// try for a shared lock
    			return setSharedLock(tid, pid);
    			
    		} else {
    			// try for an exclusive lock
    			return setExclusiveLock(tid, pid);
    			
    		}
    	}
    	

    	// sets shared lock for transaction with tid on page with pid if: 
    	// no other transaction has an xclusive lock on page
    	// returns true if lock set, else returns false
    	private synchronized boolean setSharedLock(TransactionId tid, PageId pid){
    		// try to get transaction id which holds exclusive lock on pid
    		TransactionId exclusiveTid = exclusiveLocks.get(pid);
    		
    		// get all transactions that have a shared lock on pid
    		Set<TransactionId> tidWithSharedLock = sharedLocks.get(pid);
    		
    		if(exclusiveTid == null || exclusiveTid.equals(tid)){
    			// no transaction has an exclusive lock on this page.
    			// or the same requesting transaction already has an exclusive lock on this page
    			// grant the shared lock
    						
    			if(tidWithSharedLock == null){
    				// no transactions have a shared lock on this page
    				tidWithSharedLock = new HashSet<TransactionId>();
    				
    			} 
    			
    			// update the sharedLocks Map
    			tidWithSharedLock.add(tid);
    			sharedLocks.put(pid, tidWithSharedLock);
    			
    			// updated sharedLockedPages Map
    			Set<PageId> sharedLockedPids = sharedLockedPages.get(tid);
    			if(sharedLockedPids == null){
    				sharedLockedPids = new HashSet<PageId>();
    			}
    			sharedLockedPids.add(pid);
    			
    			sharedLockedPages.put(tid, sharedLockedPids);
    			return true;
    					
    		} else {
    			// some other transaction has an exclusive lock on pid, so deny the lock
    			return false;
    		}
    		
    	}
    	
    	// sets the exclusive lock for transaction with tid on page pid if:
    	// no other transaction has a shared or exclusive lock on the page
    	// returns true if exclusive lock is granted, false otherwise
    	private synchronized boolean setExclusiveLock(TransactionId tid, PageId pid){
    		
    		Set<TransactionId> sharedLockTids = sharedLocks.get(pid);
    		TransactionId exclusiveLockTid = exclusiveLocks.get(pid);
    		
    		if(sharedLockTids != null && sharedLockTids.size() > 1){
    			// more than one transaction have a shared lock on pid, deny lock
    			return false;
    		} 
    		if(sharedLockTids != null && sharedLockTids.size() == 1 && !sharedLockTids.contains(tid)){
    			// some other transaction has a shared lock on pid
    			return false;
    		}
    		
    		if(exclusiveLockTid != null && !exclusiveLockTid.equals(tid)){
    			// some other transaction has exclusive lock on pid
    			return false;
    		} else {
    			// no other tid has exclusive lock on pid or same requesting tid has exclusive lock
    			// on pid. then grant the lock
    			exclusiveLocks.put(pid, tid);
    			
    			Set<PageId> xLockSet = exclusiveLockedPages.get(tid);
    			if(xLockSet == null){
    				xLockSet = new HashSet<PageId>();
    		
    			}
    			xLockSet.add(pid);
    			exclusiveLockedPages.put(tid, xLockSet);
    			return true;
    		}
    				
    	}
    	
    		
    	 /**
         * Releases the lock on a page.
         * @param tid the ID of the transaction requesting the unlock
         * @param pid the ID of the page to unlock
         */
    	public synchronized void releaseLock(TransactionId tid, PageId pid) {
    		
    		// release shared lock on pid held by tid, if any
    		Set<PageId> sharedLockedPids = sharedLockedPages.get(tid);
    		if(sharedLockedPids != null){
    			sharedLockedPids.remove(pid);
    			sharedLockedPages.put(tid, sharedLockedPids);
    		}
    		
    		Set<TransactionId> sharedLockTids = sharedLocks.get(pid);
    		if(sharedLockTids != null){
    			sharedLockTids.remove(tid);
    			sharedLocks.put(pid, sharedLockTids);
    		}
    		
    		// release exclusive lock on pid by tid, if any
    		Set<PageId> exclusiveLockedPid = exclusiveLockedPages.get(tid);
    		
    		if(exclusiveLockedPid != null){
    			exclusiveLockedPid.remove(pid);
    			exclusiveLockedPages.put(tid, exclusiveLockedPid);
    		}
    		
    		exclusiveLocks.remove(pid);
    	
    	}
    	
    	/**
    	 *  Return true if the specified transaction has a lock on the specified page
    	 */
    	public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
    		
    		// check for shared locks
    		Set<TransactionId> tidSet = sharedLocks.get(pid);
    		if(tidSet != null && tidSet.contains(tid)){
    			return true;
    		}
    		
    		//check for exclusive lock
    		TransactionId exclusiveLockTransaction = exclusiveLocks.get(pid);
    		if(exclusiveLockTransaction != null && exclusiveLockTransaction.equals(tid)){
    			return true;
    		}
    		
    		return false;
    	}
    	
    	/**
    	 * releases all locks held by transaction with transaction id tid
    	 * @param tid
    	 */
    	public synchronized void releaseAllLocks(TransactionId tid) {
    		
    		// release all exclusive locks
    		Set<PageId> exclusivePageIdSet = exclusiveLocks.keySet();
    		Set<PageId> exclusivePageIdSetCopy = new HashSet<PageId>();
    		for(PageId exclusivePageId : exclusivePageIdSet) {
    			exclusivePageIdSetCopy.add(exclusivePageId);
    		}
    		for(PageId pageIdToRemove : exclusivePageIdSetCopy) {
    			TransactionId transactionToRemove = exclusiveLocks.get(pageIdToRemove);
    			if(transactionToRemove != null && transactionToRemove.equals(tid)){
    				exclusiveLocks.remove(pageIdToRemove);
    			}
    		}
    		exclusiveLockedPages.remove(tid);
    		
    		
    		// release all shared locks
    		Set<PageId> sharedPageIdSet = sharedLocks.keySet();
    		for(PageId sharedPageId : sharedPageIdSet){
    			Set<TransactionId> transactionIdSet = sharedLocks.get(sharedPageId);
    			if(transactionIdSet != null){
    				transactionIdSet.remove(tid);
    				sharedLocks.put(sharedPageId, transactionIdSet);
    			}
    		}
    		sharedLockedPages.remove(tid);
    		
    	}

    }

}