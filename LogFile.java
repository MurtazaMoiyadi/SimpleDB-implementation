
package simpledb;

import java.io.*;
import java.util.*;
import java.lang.reflect.*;

/**
LogFile implements the recovery subsystem of SimpleDb.  This class is
able to write different log records as needed, but it is the
responsibility of the caller to ensure that write ahead logging and
two-phase locking discipline are followed.  <p>

<u> Locking note: </u>
<p>

Many of the methods here are synchronized (to prevent concurrent log
writes from happening); many of the methods in BufferPool are also
synchronized (for similar reasons.)  Problem is that BufferPool writes
log records (on page flushed) and the log file flushes BufferPool
pages (on checkpoints and recovery.)  This can lead to deadlock.  For
that reason, any LogFile operation that needs to access the BufferPool
must not be declared synchronized and must begin with a block like:

<p>
<pre>
    synchronized (Database.getBufferPool()) {
       synchronized (this) {

       ..

       }
    }
</pre>
*/

/**
<p> The format of the log file is as follows:

<ul>

<li> The first long integer of the file represents the offset of the
last written checkpoint, or -1 if there are no checkpoints

<li> All additional data in the log consists of log records.  Log
records are variable length.

<li> Each log record begins with an integer type and a long integer
transaction id.

<li> Each log record ends with a long integer file offset representing
the position in the log file where the record began.

<li> There are five record types: ABORT, COMMIT, UPDATE, BEGIN, and
CHECKPOINT

<li> ABORT, COMMIT, and BEGIN records contain no additional data

<li>UPDATE RECORDS consist of two entries, a before image and an
after image.  These images are serialized Page objects, and can be
accessed with the LogFile.readPageData() and LogFile.writePageData()
methods.  See LogFile.print() for an example.

<li> CHECKPOINT records consist of active transactions at the time
the checkpoint was taken and their first log record on disk.  The format
of the record is an integer count of the number of transactions, as well
as a long integer transaction id and a long integer first record offset
for each active transaction.

</ul>

*/

public class LogFile {

    File logFile;
    RandomAccessFile raf;
    Boolean recoveryUndecided; // no call to recover() and no append to log
    
    static final int ABORT_RECORD = 1;
    static final int COMMIT_RECORD = 2;
    static final int UPDATE_RECORD = 3;
    static final int BEGIN_RECORD = 4;
    static final int CHECKPOINT_RECORD = 5;
    static final long NO_CHECKPOINT_ID = -1;

    static int INT_SIZE = 4;
    static int LONG_SIZE = 8;

    long currentOffset = -1;
    int pageSize;
    int totalRecords = 0; // for PatchTest

    HashMap<Long,Long> tidToFirstLogRecord = new HashMap<Long,Long>();

    /** Constructor.
        Initialize and back the log file with the specified file.
        We're not sure yet whether the caller is creating a brand new DB,
        in which case we should ignore the log file, or whether the caller
        will eventually want to recover (after populating the Catalog).
        So we make this decision lazily: if someone calls recover(), then
        do it, while if someone starts adding log file entries, then first
        throw out the initial log file contents.

        @param f The log file's name
    */
    public LogFile(File f) throws IOException {
	this.logFile = f;
        raf = new RandomAccessFile(f, "rw");
        recoveryUndecided = true;

        // install shutdown hook to force cleanup on close
        // Runtime.getRuntime().addShutdownHook(new Thread() {
                // public void run() { shutdown(); }
            // });

        //XXX WARNING -- there is nothing that verifies that the specified
        // log file actually corresponds to the current catalog.
        // This could cause problems since we log tableids, which may or
        // may not match tableids in the current catalog.
    }

    // we're about to append a log record. if we weren't sure whether the
    // DB wants to do recovery, we're sure now -- it didn't. So truncate
    // the log.
    void preAppend() throws IOException {
        totalRecords++;
        if(recoveryUndecided){
            recoveryUndecided = false;
            raf.seek(0);
            raf.setLength(0);
            raf.writeLong(NO_CHECKPOINT_ID);
            raf.seek(raf.length());
            currentOffset = raf.getFilePointer();
        }
    }

    public int getTotalRecords() {
        return totalRecords;
    }
    
    /** Write an abort record to the log for the specified tid, force
        the log to disk, and perform a rollback
        @param tid The aborting transaction.
    */
    public void logAbort(TransactionId tid) throws IOException {
        // must have buffer pool lock before proceeding, since this
        // calls rollback

        synchronized (Database.getBufferPool()) {

            synchronized(this) {
                preAppend();
                //Debug.log("ABORT");
                //should we verify that this is a live transaction?

                // must do this here, since rollback only works for
                // live transactions (needs tidToFirstLogRecord)
                rollback(tid);

                raf.writeInt(ABORT_RECORD);
                raf.writeLong(tid.getId());
                raf.writeLong(currentOffset);
                currentOffset = raf.getFilePointer();
                force();
                tidToFirstLogRecord.remove(tid.getId());
            }
        }
    }

    /** Write a commit record to disk for the specified tid,
        and force the log to disk.

        @param tid The committing transaction.
    */
    public synchronized void logCommit(TransactionId tid) throws IOException {
        preAppend();
        Debug.log("COMMIT " + tid.getId());
        //should we verify that this is a live transaction?

        raf.writeInt(COMMIT_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();
        force();
        tidToFirstLogRecord.remove(tid.getId());
    }

    /** Write an UPDATE record to disk for the specified tid and page
        (with provided         before and after images.)
        @param tid The transaction performing the write
        @param before The before image of the page
        @param after The after image of the page

        @see simpledb.Page#getBeforeImage
    */
    public  synchronized void logWrite(TransactionId tid, Page before,
                                       Page after)
        throws IOException  {
        Debug.log("WRITE, offset = " + raf.getFilePointer());
        preAppend();
        /* update record conists of

           record type
           transaction id
           before page data (see writePageData)
           after page data
           start offset
        */
        raf.writeInt(UPDATE_RECORD);
        raf.writeLong(tid.getId());

        writePageData(raf,before);
        writePageData(raf,after);
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();

        Debug.log("WRITE OFFSET = " + currentOffset);
    }

    void writePageData(RandomAccessFile raf, Page p) throws IOException{
        PageId pid = p.getId();
        int pageInfo[] = pid.serialize();

        //page data is:
        // page class name
        // id class name
        // id class bytes
        // id class data
        // page class bytes
        // page class data

        String pageClassName = p.getClass().getName();
        String idClassName = pid.getClass().getName();

        raf.writeUTF(pageClassName);
        raf.writeUTF(idClassName);

        raf.writeInt(pageInfo.length);
        for (int i = 0; i < pageInfo.length; i++) {
            raf.writeInt(pageInfo[i]);
        }
        byte[] pageData = p.getPageData();
        raf.writeInt(pageData.length);
        raf.write(pageData);
        //        Debug.log ("WROTE PAGE DATA, CLASS = " + pageClassName + ", table = " +  pid.getTableId() + ", page = " + pid.pageno());
    }

    Page readPageData(RandomAccessFile raf) throws IOException {
        PageId pid;
        Page newPage = null;

        String pageClassName = raf.readUTF();
        String idClassName = raf.readUTF();

        try {
            Class<?> idClass = Class.forName(idClassName);
            Class<?> pageClass = Class.forName(pageClassName);

            Constructor<?>[] idConsts = idClass.getDeclaredConstructors();
            int numIdArgs = raf.readInt();
            Object idArgs[] = new Object[numIdArgs];
            for (int i = 0; i<numIdArgs;i++) {
                idArgs[i] = new Integer(raf.readInt());
            }
            pid = (PageId)idConsts[0].newInstance(idArgs);

            Constructor<?>[] pageConsts = pageClass.getDeclaredConstructors();
            int pageSize = raf.readInt();

            byte[] pageData = new byte[pageSize];
            raf.read(pageData); //read before image

            Object[] pageArgs = new Object[2];
            pageArgs[0] = pid;
            pageArgs[1] = pageData;

            newPage = (Page)pageConsts[0].newInstance(pageArgs);

            //            Debug.log("READ PAGE OF TYPE " + pageClassName + ", table = " + newPage.getId().getTableId() + ", page = " + newPage.getId().pageno());
        } catch (ClassNotFoundException e){
            e.printStackTrace();
            throw new IOException();
        } catch (InstantiationException e) {
            e.printStackTrace();
            throw new IOException();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            throw new IOException();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
            throw new IOException();
        }
        return newPage;

    }

    /** Write a BEGIN record for the specified transaction
        @param tid The transaction that is beginning

    */
    public synchronized  void logXactionBegin(TransactionId tid)
        throws IOException {
        Debug.log("BEGIN");
        if(tidToFirstLogRecord.get(tid.getId()) != null){
            System.err.printf("logXactionBegin: already began this tid\n");
            throw new IOException("double logXactionBegin()");
        }
        preAppend();
        raf.writeInt(BEGIN_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        tidToFirstLogRecord.put(tid.getId(), currentOffset);
        currentOffset = raf.getFilePointer();

        Debug.log("BEGIN OFFSET = " + currentOffset);
    }

    /** Checkpoint the log and write a checkpoint record. */
    public void logCheckpoint() throws IOException {
        //make sure we have buffer pool lock before proceeding
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                //Debug.log("CHECKPOINT, offset = " + raf.getFilePointer());
                preAppend();
                long startCpOffset, endCpOffset;
                Set<Long> keys = tidToFirstLogRecord.keySet();
                Iterator<Long> els = keys.iterator();
                force();
                Database.getBufferPool().flushAllPages();
                startCpOffset = raf.getFilePointer();
                raf.writeInt(CHECKPOINT_RECORD);
                raf.writeLong(-1); //no tid , but leave space for convenience

                //write list of outstanding transactions
                raf.writeInt(keys.size());
                while (els.hasNext()) {
                    Long key = els.next();
                    Debug.log("WRITING CHECKPOINT TRANSACTION ID: " + key);
                    raf.writeLong(key);
                    //Debug.log("WRITING CHECKPOINT TRANSACTION OFFSET: " + tidToFirstLogRecord.get(key));
                    raf.writeLong(tidToFirstLogRecord.get(key));
                }

                //once the CP is written, make sure the CP location at the
                // beginning of the log file is updated
                endCpOffset = raf.getFilePointer();
                raf.seek(0);
                raf.writeLong(startCpOffset);
                raf.seek(endCpOffset);
                raf.writeLong(currentOffset);
                currentOffset = raf.getFilePointer();
                //Debug.log("CP OFFSET = " + currentOffset);
            }
        }

        logTruncate();
    }

    /** Truncate any unneeded portion of the log to reduce its space
        consumption */
    public synchronized void logTruncate() throws IOException {
        preAppend();
        raf.seek(0);
        long cpLoc = raf.readLong();

        long minLogRecord = cpLoc;

        if (cpLoc != -1L) {
            raf.seek(cpLoc);
            int cpType = raf.readInt();
            @SuppressWarnings("unused")
            long cpTid = raf.readLong();

            if (cpType != CHECKPOINT_RECORD) {
                throw new RuntimeException("Checkpoint pointer does not point to checkpoint record");
            }

            int numOutstanding = raf.readInt();

            for (int i = 0; i < numOutstanding; i++) {
                @SuppressWarnings("unused")
                long tid = raf.readLong();
                long firstLogRecord = raf.readLong();
                if (firstLogRecord < minLogRecord) {
                    minLogRecord = firstLogRecord;
                }
            }
        }

        // we can truncate everything before minLogRecord
        File newFile = new File("logtmp" + System.currentTimeMillis());
        RandomAccessFile logNew = new RandomAccessFile(newFile, "rw");
        logNew.seek(0);
        logNew.writeLong((cpLoc - minLogRecord) + LONG_SIZE);

        raf.seek(minLogRecord);

        //have to rewrite log records since offsets are different after truncation
        while (true) {
            try {
                int type = raf.readInt();
                long record_tid = raf.readLong();
                long newStart = logNew.getFilePointer();

                Debug.log("NEW START = " + newStart);

                logNew.writeInt(type);
                logNew.writeLong(record_tid);

                switch (type) {
                case UPDATE_RECORD:
                    Page before = readPageData(raf);
                    Page after = readPageData(raf);

                    writePageData(logNew, before);
                    writePageData(logNew, after);
                    break;
                case CHECKPOINT_RECORD:
                    int numXactions = raf.readInt();
                    logNew.writeInt(numXactions);
                    while (numXactions-- > 0) {
                        long xid = raf.readLong();
                        long xoffset = raf.readLong();
                        logNew.writeLong(xid);
                        logNew.writeLong((xoffset - minLogRecord) + LONG_SIZE);
                    }
                    break;
                case BEGIN_RECORD:
                    tidToFirstLogRecord.put(record_tid,newStart);
                    break;
                }

                //all xactions finish with a pointer
                logNew.writeLong(newStart);
                raf.readLong();

            } catch (EOFException e) {
                break;
            }
        }

        Debug.log("TRUNCATING LOG;  WAS " + raf.length() + " BYTES ; NEW START : " + minLogRecord + " NEW LENGTH: " + (raf.length() - minLogRecord));

        raf.close();
        logFile.delete();
        newFile.renameTo(logFile);
        raf = new RandomAccessFile(logFile, "rw");
        raf.seek(raf.length());
        newFile.delete();

        currentOffset = raf.getFilePointer();
        //print();
    }

    /** Rollback the specified transaction, setting the state of any
        of pages it updated to their pre-updated state.  To preserve
        transaction semantics, this should not be called on
        transactions that have already committed (though this may not
        be enforced by this method.)

        @param tid The transaction to rollback
    */
    
    public void rollback(TransactionId tid)
        throws NoSuchElementException, IOException {
        synchronized (Database.getBufferPool()) {
            synchronized(this) {
               preAppend();
               
               List<PageId> checkedPages = new ArrayList<PageId>();
                
               // id of this transaction
               long tidLong = tid.getId();
               
               // go the 'begin' record for this transaction and skip it
               long recordStartOffset = tidToFirstLogRecord.get(tid.getId());
               raf.seek(recordStartOffset);
               raf.skipBytes(20);
               
               // read all records in log till end is reached
               while(raf.getFilePointer() < raf.length()){
            	   int recordType = raf.readInt();
            	   long recordTid = raf.readLong();
            	
            	   int numTids = 0;
            	   
            	   // for checkpoint record, read number of tids into recordTid
            	   if(recordType == CHECKPOINT_RECORD){
            		   numTids = raf.readInt();
            	   }
            	   
            	   if(recordTid != tidLong){
            		   // skip the record
            		   int bytesToSkip;
            		   if(recordType == CHECKPOINT_RECORD){
            			   bytesToSkip = (numTids * 16) + 8;
            			   raf.skipBytes(bytesToSkip);
            			   
            		   } else if (recordType == UPDATE_RECORD){
            			   this.readPageData(raf);
            			   this.readPageData(raf);
            			   raf.skipBytes(8);
            		   } else {
            			   raf.skipBytes(8);
            		   }
            	   } else {
            		               		   
            		   // rollback the record
            		   Page before = readPageData(raf);
            		   Page after = readPageData(raf);
            		              		   
            		   raf.skipBytes(8);
            		   
            		   if(!checkedPages.contains(before.getId())){
            			// discard the later page from the bufferpool
                		   Database.getBufferPool().discardPage(after.getId());
                		   
                		   // write old page to disk
                		   Database.getCatalog().getDbFile(before.getId().getTableId()).writePage(before);
                		   
                		   checkedPages.add(before.getId());
            			   
            		   }
            	   }
               }
            }
        }
    }
    
    /** Shutdown the logging system, writing out whatever state
        is necessary so that start up can happen quickly (without
        extensive recovery.)
    */
    public synchronized void shutdown() {
        try {
            logCheckpoint();  //simple way to shutdown is to write a checkpoint record
            raf.close();
        } catch (IOException e) {
            System.out.println("ERROR SHUTTING DOWN -- IGNORING.");
            e.printStackTrace();
        }
    }

    /** Recover the database system by ensuring that the updates of
        committed transactions are installed and that the
        updates of uncommitted transactions are not installed.
    */
    public void recover() throws IOException {
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                recoveryUndecided = false;
                
                // set of loser transactions
                Set<Long> loserTransactions = new HashSet<Long>();
                
                // offset sequence for undo actions (bottom up)
                Stack<Long> undoRecordSequence = new Stack<Long>();
                
                // offsets of loser transactions before a checkpoint
                List<Long> preCheckPtLoserOffsets = new ArrayList<Long>();
                
                // list of aborted transactions
                List<Long> abortedTransactions = new ArrayList<Long>();
                
                // maps recordId -> first offset
                Map<Long, Long> tidOffsetMap = new HashMap<Long, Long>();
                
                raf.seek(0);
                long checkpoint = raf.readLong();
                
                // skip to last checkpoint if it exists
                if(checkpoint != -1){
                	// go to checkpoint and skip first 12 bytes
                	raf.seek(checkpoint);
                	raf.skipBytes(12);
                	
                	// enter the transaction info in checkpoint in the map
                	int numCheckptTransactions = raf.readInt();
                	for(int i = 0; i < numCheckptTransactions; i++){
                		long recordId = raf.readLong();
                		long recordOffset = raf.readLong();
                		loserTransactions.add(recordId);
                		preCheckPtLoserOffsets.add(recordOffset);
                   	}
                	// skip the offset bytes in the checkpoint record
                	raf.skipBytes(8);
                }
                
                // read all records from the checkpoint onwards 
                while(raf.getFilePointer() < raf.length()) {
                    int recordType = raf.readInt();
                    
                    if(recordType == UPDATE_RECORD){
                    	// read record info
                    	Long recordId = raf.readLong();
                    	Page before = readPageData(raf);
                    	Page after = readPageData(raf);
                    	long recordOffset = raf.readLong();
                    	
                    	if(checkpoint != -1){
	                    	// save the record information
	                    	loserTransactions.add(recordId);
	                    	undoRecordSequence.push(recordOffset);
	                    	
	                    	// discard old page from buffer pool
	                        Database.getBufferPool().discardPage(before.getId());
	                        
	                        // write new page
	                    	Database.getCatalog().getDbFile(before.getId().getTableId()).writePage(after);
	                  
                    	}     
                    	
                    } else if (recordType == BEGIN_RECORD) {
                    	 Long recordId = raf.readLong();
                         long recordOffset = raf.skipBytes(8);
                         loserTransactions.add(recordId);
                         
                         tidOffsetMap.put(recordId, recordOffset);
                         
                    } else if (recordType == ABORT_RECORD) {
                    	if(checkpoint!= -1){
                    		
	                    	Long recordId = raf.readLong();
	                        loserTransactions.add(recordId);
	                        abortedTransactions.add(recordId);
	                        raf.skipBytes(8);
                    	} else {
                    		
	                        long recordId = raf.readLong();
	                        loserTransactions.remove(recordId);
	                        raf.skipBytes(8);
                    	}
                    } else if (recordType == COMMIT_RECORD) {
                    	if(checkpoint != -1){
	                    	Long recordId = raf.readLong();
	                    	loserTransactions.remove(recordId);
	                    	raf.skipBytes(8);
                    	} else {
                    		long recordId = raf.readLong();
                            loserTransactions.remove(recordId);
                            raf.skipBytes(8);
                    	}
                    }
                  
                }
                if(checkpoint != -1){
                	// perform bottom up undo on loser transactions
                	undoTransactions(loserTransactions, undoRecordSequence, preCheckPtLoserOffsets);
                } else {
                	
                	undoTransactions(loserTransactions, tidOffsetMap);
                    
                }
               
            }
         }
    }
    
    // this method performs bottom up undo operation for records in the log file
    // only if a checkpoint exists
    private void undoTransactions(Set<Long> loserTransactions,
			Stack<Long> undoRecordSequence, List<Long> preCheckPointLoserOffsets) throws IOException {
    	    	
    		while(!undoRecordSequence.isEmpty()){
    			
    			long recordOffset = undoRecordSequence.pop();
    			raf.seek(recordOffset);
    			int recordType = raf.readInt();
    			
    			// undo updates 
    			if(recordType == UPDATE_RECORD){
    				Long recordId = raf.readLong();
    				
    				if(loserTransactions.contains(recordId)){ 
    					Page page = readPageData(raf);
    					readPageData(raf);
    					
    					// discard old page
    					Database.getBufferPool().discardPage(page.getId());
             		   
             		    // write old page to disk
             		    Database.getCatalog().getDbFile(page.getId().getTableId()).writePage(page);
    					
    				}
    				
    			}
    			
    		}
    		
    		// undo on loser transactions started before the checkpoint
    		for(Long offset: preCheckPointLoserOffsets){
    			raf.seek(offset);
    			int recordType = raf.readInt();
    			Long recordId = raf.readLong();
    			raf.skipBytes(8);
    			
    			// reads records till the checkpoint is reached
    			while(recordType != CHECKPOINT_RECORD){
    				recordType = raf.readInt();
    				recordId = raf.readLong();
    				
    				if(recordType == UPDATE_RECORD && loserTransactions.contains(recordId)){
    					loserTransactions.remove(recordId);
    					Page page = readPageData(raf);
    					readPageData(raf);
    					// discard after page
    					Database.getBufferPool().discardPage(page.getId());
    	         		   
             		    // write before page to disk
             		    Database.getCatalog().getDbFile(page.getId().getTableId()).writePage(page);
    					
    				}
    			}
    		}
    }
    
    // performs undo on the loser transaction updates for the case when no checkpoint is present
	private void undoTransactions(Set<Long> loserTransactions, Map<Long, Long> recordOffsetMap) throws NoSuchElementException, IOException {
        List<PageId> checkedPages = new LinkedList<PageId>();
        
        for(long tid : loserTransactions){
        	Long recordFirstOffset = recordOffsetMap.get(tid);
	        
	        // skip the BEGIN record
	        raf.seek(recordFirstOffset);
	        raf.skipBytes(20);
	        
	        // read all records in the log
	        
	        while(raf.getFilePointer() < raf.length()) {
	            int recordType = raf.readInt();
	            Long recordId = raf.readLong();
	            if (recordType == LogFile.UPDATE_RECORD) {
	                
					if(recordId == tid) {
					    Page page = readPageData(raf);
					    readPageData(raf);
					    raf.skipBytes(8);
					    if(!checkedPages.contains(page.getId())) {
					        Database.getBufferPool().discardPage(page.getId());
					        Database.getCatalog().getDbFile(page.getId().getTableId()).writePage(page);
					        checkedPages.add(page.getId());
					    }
					}
					else {
					    readPageData(raf);
					    readPageData(raf);
					    raf.skipBytes(8);
					}
					
				} else if (recordType == CHECKPOINT_RECORD) {
	                int numOfTransactions = raf.readInt();
	                raf.skipBytes((16 * numOfTransactions) + 8);
	                
	            } else if(recordType == ABORT_RECORD || recordType == BEGIN_RECORD || recordType == COMMIT_RECORD) {
	                raf.skipBytes(8);
	                
	            }  
	        } 
	   
        } 
    }

	/** Print out a human readable represenation of the log */
    public void print() throws IOException
    {
        long curOffset = raf.getFilePointer();

        raf.seek(0);

        System.out.println("0: checkpoint record at offset " + raf.readLong());

        while (true) {
            try {
                int cpType = raf.readInt();
                long cpTid = raf.readLong();

                System.out.println((raf.getFilePointer() - (INT_SIZE + LONG_SIZE)) + ": RECORD TYPE " + cpType);
                System.out.println((raf.getFilePointer() - LONG_SIZE) + ": TID " + cpTid);

                switch (cpType) {
                case BEGIN_RECORD:
                    System.out.println(" (BEGIN)");
                    System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
                    break;
                case ABORT_RECORD:
                    System.out.println(" (ABORT)");
                    System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
                    break;
                case COMMIT_RECORD:
                    System.out.println(" (COMMIT)");
                    System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
                    break;

                case CHECKPOINT_RECORD:
                    System.out.println(" (CHECKPOINT)");
                    int numTransactions = raf.readInt();
                    System.out.println((raf.getFilePointer() - INT_SIZE) + ": NUMBER OF OUTSTANDING RECORDS: " + numTransactions);

                    while (numTransactions-- > 0) {
                        long tid = raf.readLong();
                        long firstRecord = raf.readLong();
                        System.out.println((raf.getFilePointer() - (LONG_SIZE + LONG_SIZE)) + ": TID: " + tid);
                        System.out.println((raf.getFilePointer() - LONG_SIZE) + ": FIRST LOG RECORD: " + firstRecord);
                    }
                    System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());

                    break;
                case UPDATE_RECORD:
                    System.out.println(" (UPDATE)");

                    long start = raf.getFilePointer();
                    Page before = readPageData(raf);

                    long middle = raf.getFilePointer();
                    Page after = readPageData(raf);

                    System.out.println(start + ": before image table id " + before.getId().getTableId());
                    System.out.println((start + INT_SIZE) + ": before image page number " + before.getId().pageNumber());
                    System.out.println((start + INT_SIZE) + " TO " + (middle - INT_SIZE) + ": page data");

                    System.out.println(middle + ": after image table id " + after.getId().getTableId());
                    System.out.println((middle + INT_SIZE) + ": after image page number " + after.getId().pageNumber());
                    System.out.println((middle + INT_SIZE) + " TO " + (raf.getFilePointer()) + ": page data");

                    System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());

                    break;
                }

            } catch (EOFException e) {
                //e.printStackTrace();
                break;
            }
        }

        // Return the file pointer to its original position
        raf.seek(curOffset);
    } 

    public  synchronized void force() throws IOException {
        raf.getChannel().force(true);
    }

}