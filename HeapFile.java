package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	
	/**
	 * Tuple descriptor for this HeapFile/Table
	 * 
	 */
	private TupleDesc td;
	
	/**
	 * Data file
	 */
	File file;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
    	this.td = td;
    	this.file = f;
           	
    }
    
    public HeapFile(File f)
    {
    	this.file=f;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

       
    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        
    	// table id of required page does not match the table id of this file
    	if(pid.getTableId() != getId()) {
    		throw new IllegalArgumentException("Page does not exist in this file");
    	}
    	try {
			RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
			
			// array to store page data
			byte[] pageData = new byte[BufferPool.PAGE_SIZE];
			
			// get offset into the random access file
			int offset = pid.pageNumber() * BufferPool.PAGE_SIZE;
			randomAccessFile.seek(offset);
			
			// read the page data
			randomAccessFile.read(pageData, 0, BufferPool.PAGE_SIZE);
			randomAccessFile.close();
			return new HeapPage((HeapPageId)pid, pageData);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		// we should not reach here
		throw new IllegalArgumentException();
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int)Math.ceil(file.length()/BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        
    	// get a list of pages in this file
    	//List<HeapPage> pages = new ArrayList<HeapPage>();
    	/*    	
    	for(int i = 0; i < numPages(); i++){
    		// page id has value of i
    		// first page id == 0, second page id == 1, ...
    		PageId pageId = new HeapPageId(getId(), i);
    		
    		try {
    			
    			// try getting the page from the BufferPool, and if it does not exist there,
    			// BufferPool will try to get it from disk
				Page page = Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
    			pages.add((HeapPage)page);
			} catch (TransactionAbortedException e) {
				e.printStackTrace();
			} catch (DbException e) {
				e.printStackTrace();
			}
    	}
    	
    	// get a list of all tuples from all the pages
    	List<Tuple> tupleList = new LinkedList<Tuple>();
    	for(HeapPage p : pages){
    		
    		// this page iterator will not return empty slots in the page
    		// see implementation of the iterator() method in HeapPage
    		Iterator<Tuple> itr = p.iterator();
    		while(itr.hasNext()){
    			tupleList.add(itr.next());
    		}
    	}*/
    	
    	return new HeapFileTupleIterator(tid);
    	
    }
    // Private inner class that implements an iterator for this
    // HeapFile. The iterator iterates over all the tuples in 
    // the HeapFile
    private class HeapFileTupleIterator implements DbFileIterator {
    	
    	private static final long serialVersionUID = 1L;
    	
    	// iterator
    	private Iterator<Tuple> i = null;
    	
    	// tuples to iterate over
    	private Iterable<Tuple> tuples = null;
    	
    	// id of the transaction that makes request for iterator
    	private TransactionId tid;
    	
    	// page number in HeapFile being iterated over
    	private int pageNo;
    	 
    	
        public HeapFileTupleIterator(TransactionId tid) {
        	this.tid = tid;
        	 
        }
    	    
    	@Override
    	public void open() throws DbException, TransactionAbortedException {
    		
           	// get tuples from the first page in the file
    		pageNo = 0;
    		tuples = getTuplesFromPage(pageNo);
    		i = tuples.iterator();
        }

    	@Override
    	public boolean hasNext() throws DbException, TransactionAbortedException {
    		if( i == null){
    			return false;
    		}
    		//return (i.hasNext() || (!i.hasNext() && pageNo < numPages()-1));
    		
    		if(i.hasNext()){
    			// next tuple found in this page
    			return true;
    		} else if (!i.hasNext() && pageNo < numPages()-1){
    			// no tuple in this page but more pages exist
    			List<Tuple> nextPgTuples = getTuplesFromPage(pageNo + 1);
    			if(nextPgTuples.size() != 0){
    				return true;
    			} else {
    				return false;
    			}
    		} else {
    			// no tuple on this page and no more pages
    			return false;
    		}
    	}

    	//@Override
    	public Tuple next() throws DbException, TransactionAbortedException,
    			NoSuchElementException {
    		
    		// check if open() called before
    		if(i == null){
    			throw new NoSuchElementException("open() not called on iterator");
    		}
    		
    		if(i.hasNext()){
    			// tuples present on current page
    			Tuple t = i.next();
    			return t;
    		} else if(!i.hasNext() && pageNo < numPages()-1) {
    			// no tuples on current pages, but more pages available
    			// get the tuples from next page
    			pageNo ++;
    			tuples = getTuplesFromPage(pageNo);
        		i = tuples.iterator();
        		if (i.hasNext())
        		 return i.next();
        		else {
        			throw new NoSuchElementException("No more Tuples");
        		}
            	
    		} else {
    			// no more tuples on current page and no more pages in file
    			throw new NoSuchElementException("No more Tuples");
    		}
    		
    	}
    	
    	// Returns a list of tuples from page 
    	// @param pageNum the number of the page in the heap file
    	//        from which tuples are returned
    	// @return list of tuples from page
        // @throws TransactionAbortedException, DbException
    	private List<Tuple> getTuplesFromPage(int pageNum) throws TransactionAbortedException, DbException{
    		
    		PageId pageId = new HeapPageId(getId(), pageNum);
        	Page page = Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
							
			List<Tuple> tupleList = new ArrayList<Tuple>();
			
			// get all tuples from the first page in the file
			// using the HeapPage iterator
			HeapPage hp = (HeapPage)page;
			Iterator<Tuple> itr = hp.iterator();
    		while(itr.hasNext()){
    			tupleList.add(itr.next());
    		}
    		return  tupleList;
    	}

    	//@Override
    	public void rewind() throws DbException, TransactionAbortedException {
    		close();
            open();

    	}

    	//@Override
    	public void close() {
    		i = null;

    	}

    }
   


}
