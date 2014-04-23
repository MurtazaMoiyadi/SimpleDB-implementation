package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 */

public class Catalog {

	/**
	 * List of tables in database
	 */
	private Map<Integer, Table> tableMap;
	protected Hashtable<Integer,Table> idToTable;
	protected List<Table> tables;
	
    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
       tableMap = new HashMap<Integer, Table>();
       this.idToTable = new Hashtable<Integer,Table>();
       this.tables = new ArrayList<Table>();
    }
        
    /**
     * returns list of tables in the catalog
     */
    public List<Table> getTables() {
    	
    	List<Table> tableList = new LinkedList<Table>();
    	Collection<Table> catalogMapValues = tableMap.values();
    	
    	for(Table t : catalogMapValues) {
    		tableList.add(t);
    	}
    	return tableList;
    }
    
    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * @param pkeyField the name of the primary key field
     * conflict exists, use the last table to be added as the table for a given name.
     */
    /*private void addTable(DbFile file, String name, String pkeyField) {
        
    	if (name == null) {
    		throw new IllegalArgumentException("Table name is null");
    	}
    	
    	// if conflict exists, use the last table to be added as the table for a given name.
    	Table t = new Table(file, name, pkeyField);
    	if (tableMap.containsValue(t)) {
    		tableMap.remove(file.getId());
    	}
    	
    	System.out.println("Adding ----- " + file.getId());
    	tableMap.put(file.getId(), t);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    } */
    
    public void addTable(DbFile file, TupleDesc desc) {
    	addTable(file, desc, "");
    }
    
    public void addTable(DbFile file, TupleDesc desc, String name) {
    	if (desc != null) {
    		Table table = new Table(file,desc,name);
        	this.tableMap.put(file.getId(), table);
        	this.tables.add(table);
    	}
    	
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    /*public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }*/

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        
    	Collection<Table> tablesInCatalog = tableMap.values();
        for(Table t : tablesInCatalog) {
        	if (t.getTableName().equals(name)) {
        		return t.getFile().getId();
        	}
        }
        // table does not exist
        throw new NoSuchElementException("Table does not exist in Catalog");
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        
    	Table t = tableMap.get(tableid);
    	if (t == null) {
    		throw new NoSuchElementException("Table does not exist in Catalog");
    	}
    	return t.getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDbFile(int tableid) throws NoSuchElementException {
        
    	Table t = tableMap.get(tableid);
    	if (t == null) {
    		throw new NoSuchElementException("Table does not exist in Catalog ");
    	}
    	return t.getFile();
    }

    public String getPrimaryKey(int tableid) {
        
    	Table t = tableMap.get(tableid);
    	if (t == null) {
    		throw new NoSuchElementException("Table does not exist in Catalog");
    	}
    	return t.getPkeyField();
    }

    public Iterator<Integer> tableIdIterator() {
        
    	return tableMap.keySet().iterator();
    }

    public String getTableName(int id) {
       
    	Table t = tableMap.get(id);
    	if (t == null) {
    		throw new NoSuchElementException("Table does not exist in Catalog");
    	}
    	return t.getTableName();
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        
    	tableMap.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf, t, name);
                //addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}
