package simpledb;

/**
 * This class represents a Table in SimpleDB
 */

public class Table {
	
	/**
     * The DbFile which stores the contents of the table
     */
    private DbFile file;
    
    /**
     *  Name of Table
     */
    private String tableName;
    
    /**
     *  Primary key
     */
    private String pkeyField;
    
    private TupleDesc tupleDesc;
    
    /**
     * Constructs a new Table
     */
    public Table(DbFile file, String tableName, String pkeyField){
    	this.file = file;
    	this.tableName = tableName;
    	this.pkeyField = pkeyField;   	

    }
    
    public Table(DbFile file, TupleDesc tupleDesc, String name) {
		super();
		this.file = file;
		this.tupleDesc = tupleDesc;
		this.tableName = name;
	}
    
        
    /**
     * 
     * @return file
     */
    public DbFile getFile(){
    	return file;
    }
    
    /**
     * 
     * @return tableName
     */
    public String getTableName(){
    	return tableName;
    }
    
    /**
     * 
     * @return pkeyField
     */
    public String getPkeyField(){
    	return pkeyField;
    }
    
    public TupleDesc getTupleDesc() {
		return tupleDesc;
	}

	/**
     * Compares the specified object with this Table for equality. Two
     * Tables are considered equal if they have the same name.
     * 
     * @param o
     *            the Object to be compared for equality with this Table.
     * @return true if the object is equal to this Table
     */
    public boolean equals(Object o) {
    	if(o instanceof Table){
    		Table otherTable = (Table)o;
    		if(this.tableName.equals(otherTable.tableName)){
    			return true;
    		} else {
    			return false;
    		}
    	} else { 
    		return false;
    	}
    	
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        // throw new UnsupportedOperationException("unimplemented");
    	return file.hashCode() + tableName.hashCode() + pkeyField.hashCode();
    }
    

}