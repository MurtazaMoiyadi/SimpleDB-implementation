package simpledb;

import java.io.Serializable;
import java.util.*;

import simpledb.TupleDesc.TDItem;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
	
	/**
	 * List of attributes of the schema
	 */
	private List<TDItem> attributeList;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        Iterator<TDItem> itr = attributeList.iterator();
        return itr;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        attributeList = new ArrayList<TDItem>();
        for(int i = 0; i < typeAr.length; i++){
        	TDItem attribute = new TDItem(typeAr[i],fieldAr[i]);
        	attributeList.add(attribute);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        attributeList = new ArrayList<TDItem>();
        for(int i = 0; i < typeAr.length; i++){
        	TDItem attribute = new TDItem(typeAr[i], null);
        	attributeList.add(attribute);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return attributeList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
    	
        if(i < 0 || i >= attributeList.size()){
        	
        	throw new NoSuchElementException("invalid index: " + i);
        	
        }
        /*
        for(int j = 0; j < attributeList.size(); j++){
        	System.out.println(j + " :" + attributeList.get(j).fieldName);
        }*/
        return attributeList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	if(i < 0 || i > attributeList.size()-1){
        	throw new NoSuchElementException("invalid index i");
        }
        return attributeList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        
    	for(TDItem tdi : attributeList){
        	int attributeIndex = attributeList.indexOf(tdi);
        	
        	if(tdi.fieldName != null && tdi.fieldName.equals(name)){
        		return attributeIndex;
        	}
        	
        }
        
        // no matching field name found
        throw new NoSuchElementException("no field with matching name found");
        
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	int size = 0;
    	for(TDItem tdi : attributeList){
    		if(tdi.fieldType == Type.INT_TYPE){
    			size = size + 4;
    		} else {
    			size = size + Type.STRING_LEN;
    		}
    	}
    	return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    	// create arrays to hold field name and field type from each TupleDesc
    	Type[] typeAr = new Type[td1.numFields() + td2.numFields()];
    	String[] fieldAr = new String[td1.numFields() + td2.numFields()];
    	
    	int index;
    	
    	for(index = 0; index < td1.numFields(); index++){
    		typeAr[index] = td1.getFieldType(index);
    		fieldAr[index] = td1.getFieldName(index);
    	}
    	
    	for(int j = 0; j < td2.numFields(); j++){
    		typeAr[index] = td2.getFieldType(j);
    		fieldAr[index] = td2.getFieldName(j);
    		index++;
    	}
    	
    	//create a merged TupleDesc
    	TupleDesc mergedTupleDesc = new TupleDesc(typeAr, fieldAr);
    	
        return mergedTupleDesc;
        
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	
        if(o instanceof TupleDesc){
        	TupleDesc other = (TupleDesc)o;
        	if(this.numFields() != other.numFields()){
        		// size mismatch
        		return false;
        	} else {
        		for(int i = 0; i < this.numFields(); i++){
        			if(!this.getFieldType(i).equals(other.getFieldType(i))){
        				// attribute type mismatch
        				return false;
        			}
        		}
        		return true;
        	}
                	
        } else {
        	// type mismatch
        	return false;
        }
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return 1;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
    	String result = "";
    	String type = "";
        for(int i = 0; i < this.numFields()-1; i++){
        	
        	if(this.getFieldType(i) == Type.INT_TYPE){
        		type = "INT";
        	} else {
        		type = "STRING";
        	}
        	result = result + type + "(" + this.getFieldName(i) + "), "; 
        }
        int finalIndex = this.numFields()-1;
        if(this.getFieldType(finalIndex) == Type.INT_TYPE){
    		type = "INT";
    	} else {
    		type = "STRING";
    	}
        result = result + type + "(" + this.getFieldName(finalIndex) + ")"; 
        return result;
    }
}