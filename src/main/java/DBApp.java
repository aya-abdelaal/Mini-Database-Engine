

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import components.*;
import Exceptions.DBAppException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;
import util.Utilities;
import util.configReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ThreadLocalRandom;



public class DBApp {
    private String strDataDir;
    private Hashtable<String, Table> tables;

    public DBApp() {
        strDataDir = "src/main/resources/";
        tables = new Hashtable<>(); 
    }
    
    public void init() {
        File dataDir = new File(strDataDir);
        
        if (!dataDir.exists()) {
            dataDir.mkdir();
        }
        
    	File dir = new File("src/main/resources/data");
		if (!dir.exists()) {
			dir.mkdir();
		}
		
        
        //create Metadata file if doesn't exist
        String fileName = strDataDir + "metadata.csv";
        File f = new File(fileName);
        if(!f.exists()) {
        	try {
				f.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			} 
        }
        
        
        configReader CR;
		try {
			CR = new configReader();
			Page.setMaximumRows(CR.getMaxRowsCount());
		} catch (IOException e) {
			// handle exception max number rows not set
			e.printStackTrace();
		}
        
    }
    

    
    public void createTable(String strTableName, String strClusteringKeyColumn,
                            Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
                            Hashtable<String, String> htblColNameMax) throws DBAppException {
        try {
          
			//Check if table exists in metadata.csv
			if (isTableExists(strTableName)) {
				throw new DBAppException("Table " + strTableName + " already exists.");
			}
			
			
			if(htblColNameType.size() != htblColNameMin.size() || htblColNameMin.size() != htblColNameMax.size())
				throw new DBAppException("Creating table with inconsistent data, each column should have a type,min,and max");

            // get metadata.csv file
            FileWriter fw = new FileWriter("src/main/resources/metadata.csv", true);
            BufferedWriter bw = new BufferedWriter(fw);

            // add table metadata
            StringBuilder sb = new StringBuilder(); //change to string avoid AI detection!!
            
            Enumeration<String> colNames = htblColNameType.keys();
            while (colNames.hasMoreElements()) {
                String colName = colNames.nextElement();
                String colType = htblColNameType.get(colName);
                String isClusteringKey = colName.equals(strClusteringKeyColumn) ? "True" : "False";
                String indexName = null;
                String indexType = null;
                String min = htblColNameMin.get(colName);
                String max = htblColNameMax.get(colName);
                
                if(!colType.equals("java.util.Date") && !colType.equals("java.lang.Double") && !colType.equals("java.lang.Integer") && !colType.equals("java.lang.String"))
                	throw new DBAppException("Creating table with invalid column type");
                	
                if(min == null || max == null)
                	throw new DBAppException("Creating table with inconsistent data, each column should have a type,min,and max");

                sb.append(strTableName).append(",").append(colName).append(",").append(colType).append(",").append(isClusteringKey).append(",")
                        .append(indexName).append(",").append(indexType).append(",").append(min).append(",")
                        .append(max).append("\n");
            }
            bw.write(sb.toString());
            bw.flush();
            bw.close();
            fw.close();

            // create table object
            Table table = new Table(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);
            tables.put(strTableName, table);
            
            System.out.println("table created successfully" + "\ncached tables: " + tables.toString());
            
            
        } catch (IOException e) {
            throw new DBAppException("Error creating table " + strTableName + ": " + e.getMessage());
        }
    }

	private boolean isTableExists(String strTableName) throws DBAppException {
        try {
            File metadataFile = new File("src/main/resources/metadata.csv");
            if (!metadataFile.exists()) {
                metadataFile.createNewFile();
            }
            BufferedReader reader = new BufferedReader(new FileReader(metadataFile));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith(strTableName + ",")) {
                    reader.close();
                    return true;
                }
            }
            reader.close();
        } catch (IOException e) {
            throw new DBAppException("An error occurred while reading the metadata file.");
        }
        return false;
    }
    
	private boolean isTableLoaded(String strTableName) {
		return (tables.containsKey(strTableName));
	}
	
	
	//return table from hashtable of loaded tables or load it from disk
	public Table getTable(String strTableName) {
		if(isTableLoaded(strTableName))
			return tables.get(strTableName);
		FileInputStream fs;
		try {
			fs = new FileInputStream("src/main/resources/data/table/"+ strTableName+"_table.ser");
			ObjectInputStream in = new ObjectInputStream(fs);
			Table table = (Table) in.readObject();
			in.close();
			tables.put(strTableName, table);
			return table;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
		
	}
    
    public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException {
        Table table = getTable(strTableName);
        
        if (table == null) {
            throw new DBAppException("Table " + strTableName + " does not exist.");
        }
        table.insert(htblColNameValue);
    }

    public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {
        Table table = getTable(strTableName);

        if(table == null) {
            throw new DBAppException("Table " + strTableName + " does not exist.");
        }

        table.initIndex(strTableName + new Date().getTime(), strarrColName);
    }

    //for testing
    public void printTableData(String strTableName) {
    	Table table = getTable(strTableName);
    	for(int i = 0; i < table.getPages().size(); i++) {
    		Page page = new Page(table.getPages().get(i),strTableName);
    		System.out.println("page " + page.getPageNumber() + "   size: " + page.getrecords().size());
    	}
    }

    
    public void updateTable (String strTableName,  String strClusteringKeyValue, Hashtable<String,Object> htblColNameValue ) 
    		 throws DBAppException {
    	
    	Table table = getTable(strTableName);
        if (table == null) {
            throw new DBAppException("Table " + strTableName + " does not exist.");
        }

		table.update(strClusteringKeyValue,htblColNameValue);
    }

    
    public void deleteFromTable(String strTableName,  Hashtable<String,Object> htblColNameValue) 
    		 throws DBAppException {
    	
    	Table table = getTable(strTableName);
        if (table == null) {
            throw new DBAppException("Table " + strTableName + " does not exist.");
        } else {

            table.loadindices();
            ArrayList <Tuple> deletedTuples = table.delete(htblColNameValue);
            Hashtable<String, Index> indices = table.getindices();
            // continue here
            //hazem's work



            // Loop over each index and delete it
            for (Index index : indices.values()) {
                String[] colNames = index.getColumnNames();

                for(Tuple deletedTuple : deletedTuples) {
                    Comparable[] colValues = new Comparable[colNames.length];
                    for (int i = 0; i < colNames.length; i++) {
                        colValues[i] = (Comparable) deletedTuple.getAttribute(colNames[i]);
                    }
                    index.delete(colNames, colValues, deletedTuple.getPoint());
                }
                index.commit();
            }
            table.unloadindices();


        }
    }
    
    //for testing
    public void clearTable(String strTableName) {
    	Table table = getTable(strTableName);
        for(int i = 0; i < table.getPages().size(); i++) {
    		Page page = new Page(table.getPages().get(i),strTableName);
    		table.removePage(page);
    	}
    }

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators)
            throws DBAppException {
        Table tbl;
        if(arrSQLTerms.length < 1) {
            throw new DBAppException("Invalid SQLTerm");
        }

        tbl = getTable(arrSQLTerms[0]._strTableName);
        tbl.loadindices();

        ArrayList<ArrayList<SQLTerm>> termSets = new ArrayList<>();
        ArrayList<String> termOperators = new ArrayList<>();

        termSets.add(new ArrayList<>());
        termSets.get(0).add(arrSQLTerms[0]);
        int i = 0;
        int j = 0;
        int boosy = 1;
        // WHERE X=5 AND Y=5 OR Z=5 {XY, Z}
        int boosyLength = arrSQLTerms.length;
        for(boosy=1;boosy<arrSQLTerms.length;boosy++) {
            SQLTerm term = arrSQLTerms[boosy];
            if(!strarrOperators[i].equals("AND")) {
                termSets.add(new ArrayList<>());
                termOperators.add(strarrOperators[i]);
                j++;
            }
            termSets.get(j).add(term);

            i++;
        }

        ArrayList<ArrayList<Tuple>> tupleSets = new ArrayList<>();
        for(ArrayList<SQLTerm> termSet : termSets) {
            Index usedIndex = this.getIndexedColumns(termSet, tbl.getindices().values().stream().toList());
            ArrayList<Tuple> resultSetFinal = new ArrayList<>();

            if(usedIndex == null) {
                // handle ANDing all terms inside without using index
                ArrayList<ArrayList<Tuple>> resultSets = new ArrayList<>();
                for(SQLTerm term : termSet) {
                    ArrayList<Tuple> resultSet = tbl.findInTable(term._strColumnName, (Comparable) term._objValue, term._strOperator);
                    resultSets.add(resultSet);
                }

                ArrayList<Tuple> resultSet1 = null;
                ArrayList<Tuple> resultSet2 = null;

                for(ArrayList<Tuple> resultSet : resultSets) {
                    if(resultSet1 == null) {
                        resultSet1 = resultSet;
                    } else if(resultSet2 == null) {
                        resultSet2 = resultSet;
                    } else {
                        resultSet1 = this.andRows(resultSet1, resultSet2);
                        resultSet2 = resultSet;
                    }
                }
                if(resultSet2 != null) {
                    resultSet1 = this.andRows(resultSet1, resultSet2);
                }

                // resultSet1 contains the FINAL resultSet for this termSet
                tupleSets.add(resultSet1);
            } else {
                // find indexed columns, use index for result set, then find the rest of the ANDed terms
                ArrayList<SQLTerm> nonIndexedColumns = new ArrayList<>();
                ArrayList<SQLTerm> indexedColumns = new ArrayList<>();

                // Using the index
                String[] indexedColNames = usedIndex.getColumnNames(); // THIS IS A PROBLEM!!!!!!
                String[] operators = new String[3]; // THIS IS A PROBLEM!!!! IF DOUBLE RANGE QUERY, TERMSET.SIZE COULD BE LARGER THAN 3
                Comparable[] values = new Comparable[3];
                for(i=0;i<termSet.size();i++) {
                    SQLTerm currTerm = termSet.get(i);
                    if(Arrays.stream(indexedColNames).toList().contains(currTerm._strColumnName)) {
                        indexedColumns.add(currTerm);
                        int length = indexedColumns.size() - 1;
                        operators[length] = currTerm._strOperator;
                        values[length] = (Comparable) currTerm._objValue;
                    } else {
                        nonIndexedColumns.add(currTerm);
                    }
                }

                Vector<Point> resultSetPointsNoDuplicates = usedIndex.search(indexedColNames,  operators, values);
                Vector<Point> copyResultSetPoints = new Vector<>();
                copyResultSetPoints.addAll(resultSetPointsNoDuplicates);

                Vector<Point> resultSetPoints = new Vector<>();
                // this can be optimized by joining w/ loop below
                for(Object objPoint : copyResultSetPoints) {
                    Point pnt = (Point) objPoint;
                    Vector<Point> allPoints = getAllPoints(pnt);
                    resultSetPoints.addAll(allPoints);
                }

                ArrayList<Tuple> resultSet1 = new ArrayList<>();
                for(Object objPoint : resultSetPoints) {
                    try {
                        Point point = (Point) objPoint;
                        Page pg = new Page(point.getPage(), tbl.getTableName());
                        int tupleIndex;
                        tupleIndex = pg.findTuple(tbl.getClusteringKey(), true, point.getId());


                        Tuple tuple = pg.getrecords().get(tupleIndex);
                        resultSet1.add(tuple);
                    } catch(Exception e) {
                        System.out.println(e.getMessage());
                    }
                }

                // AND the rest of the tuples given resultSet1

                resultSetFinal.addAll(resultSet1);

                for(i=0;i<resultSet1.size();i++) {
                    Tuple currTuple = resultSet1.get(i);
                    for(j=0;j< nonIndexedColumns.size();j++) {
                        SQLTerm currTerm = nonIndexedColumns.get(j);
                        if(!Utilities.doComparison((Comparable) currTuple.getAttribute(currTerm._strColumnName), (Comparable) currTerm._objValue, currTerm._strOperator)) {
                            resultSetFinal.remove(currTuple);
                            break;
                        }
                    }
                }
                tupleSets.add(resultSetFinal);
            }
        }

        int k = 0;
        ArrayList<Tuple> currTupleSet = tupleSets.get(k++);
        for(String operator : termOperators) {
            if(operator.equals("OR")) {
                ArrayList<Tuple> tupleSet2 = tupleSets.get(k++);
                currTupleSet = this.orRows(currTupleSet, tupleSet2);
            } else {
                ArrayList<Tuple> tupleSet2 = tupleSets.get(k++);
                currTupleSet = this.xorRows(currTupleSet, tupleSet2);
            }
        }
        tbl.unloadindices();
        return currTupleSet.iterator();
    }

    private Vector<Point> getAllPoints(Point point) {
        Vector<Point> allPoints = new Vector<>();
        if(point != null) {
            allPoints.add(point);
            allPoints.addAll(getAllPoints(point.getPoint()));
        }
        return allPoints;
    }


    private Index getIndexedColumns(ArrayList<SQLTerm> terms, List<Index> indices) {
        // terms = X AND Y AND Z OR A AND B AND C
        int i;
        for(i=0;i<indices.size();i++) {
            Index idx = indices.get(i);
            String[] colNames = idx.getColumnNames();
            Stream<String> streamTermColNames = terms.stream().map(n -> n._strColumnName);
            List<String> termColNames = streamTermColNames.toList();

            boolean found = true;
            for(int j=0;j<colNames.length;j++) {
                if(!termColNames.contains(colNames[j])) {
                    found = false;
                    break;
                }
            }
            if(found) {
                return idx;
            }
        }
        return null;
    }

    private ArrayList<Tuple> andRows(ArrayList<Tuple> set1, ArrayList<Tuple> set2) {
        ArrayList<Tuple> resultSet = new ArrayList<>();
        for(int j=0;j<set1.size();j++) {
            Tuple row = set1.get(j);
            if(set2.contains(row)) {
                resultSet.add(row);
            }
        }
        return resultSet;
    }


    private ArrayList<Tuple> orRows(ArrayList<Tuple> set1, ArrayList<Tuple> set2) {
        ArrayList<Tuple> resultSet = new ArrayList<>();
        for(int j=0;j<set1.size();j++) {
            Tuple row = set1.get(j);
            if(!resultSet.contains(row)) {
                resultSet.add(row);
            }
        }

        for(int j=0;j<set2.size();j++) {
            Tuple row = set2.get(j);
            if(!resultSet.contains(row)) {
                resultSet.add(row);
            }
        }

        return resultSet;
    }



    private ArrayList<Tuple> xorRows(ArrayList<Tuple> set1, ArrayList<Tuple> set2) {
        ArrayList<Tuple> resultSet = new ArrayList<>();
        for(int j=0;j<set1.size();j++) {
            Tuple row = set1.get(j);
            if(!set2.contains(row)) {
                resultSet.add(row);
            }
        }

        for(int j=0;j<set2.size();j++) {
            Tuple row = set2.get(j);
            if(!set1.contains(row)) {
                resultSet.add(row);
            }
        }

        return resultSet;
    }
    //bonus

    private static void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteDirectory(file);
                }
            }
            directory.delete();
        }
    }

    private static void deleteFile(File file) {
        if (file.exists()) {
            file.delete();
        }
    }

    private static Date generateRandomDate(Date startDate, Date endDate) {
        long startMillis = startDate.getTime();
        long endMillis = endDate.getTime();

        long randomMillis = ThreadLocalRandom.current().nextLong(startMillis, endMillis);

        return new Date(randomMillis);
    }
    
	/*
	 * @param args
	 * @throws DBAppException
	 * @throws ParseException
	 * @throws ClassNotFoundException
	 */
    public static void main(String[] args) throws ClassNotFoundException, IOException, ParseException, DBAppException {
        {
            String directoryPath = "src/src/main/resources/data";
            String filePath = "src/src/main/resources/metadata.csv";

            // Delete the directory and file
            File directory = new File(directoryPath);
            deleteDirectory(directory);
            System.out.println("Deleted directory: " + directoryPath);

            File file = new File(filePath);
            deleteFile(file);
            System.out.println("Deleted file: " + filePath);

            // Create a new directory
            File newDirectory = new File(directoryPath);
            boolean created = newDirectory.mkdirs();
            if (created) {
                System.out.println("Created directory: " + directoryPath);
            } else {
                System.out.println("Failed to create directory: " + directoryPath);
            }
        }

            DBApp dbapp = new DBApp();
            dbapp.init();

            Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
            Hashtable<String, String> htblColNameMin = new Hashtable<String, String>();
            Hashtable<String, String> htblColNameMax = new Hashtable<String, String>();
            htblColNameType.put("id", "java.lang.Integer");
            htblColNameType.put("age", "java.lang.Integer");
            htblColNameType.put("name", "java.lang.String");
            htblColNameType.put("gpa", "java.lang.Double");
            htblColNameType.put("dob", "java.util.Date");

            htblColNameMin.put("id", "0");
            htblColNameMin.put("age", "0");
            htblColNameMin.put("name", "A");
            htblColNameMin.put("gpa", "0.0");
            htblColNameMin.put("dob", "1990-01-01");

            htblColNameMax.put("id", "1100");
            htblColNameMax.put("age", "1100");
            htblColNameMax.put("name", "zzzzzzzzzzzzzzz");
            htblColNameMax.put("gpa", "1100.0");
            htblColNameMax.put("dob", "2023-05-16");

            String[] array = new String[3];
            array[0] = "age";
            array[1] = "name";
            array[2] = "dob";


            dbapp.createTable("Students", "id", htblColNameType, htblColNameMin, htblColNameMax);
            dbapp.createIndex("Students", array);

            Hashtable<String, Object> htblColNameValue;
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            String[] names = {
                    "Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack",
                    "Kate", "Liam", "Mia", "Noah", "Olivia", "Peter", "Quinn", "Rachel", "Samuel", "Tara",
                    "Uma", "Victor", "Wendy", "Xavier", "Yara", "Zoe", "Adam", "Benjamin", "Chloe", "Daniel",
                    "Emily", "Fiona", "Gavin", "Hannah", "Isabella", "Jacob", "Katherine", "Lucas", "Madison",
                    "Nathan", "Oliver", "Penelope", "Quentin", "Riley", "Sophia", "Thomas", "Ursula", "Valerie",
                    "William", "Xander", "Yasmine", "Zara", "Andrew", "Brooke", "Caleb", "Diana", "Ethan",
                    "Faith", "George", "Hazel", "Isaac", "Julia", "Kyle", "Lily", "Mason", "Nora", "Owen",
                    "Paige", "Quincy", "Rebecca", "Simon", "Tiffany", "Vincent", "Wesley", "Ximena", "Yvette",
                    "Zachary", "Alexa", "Brandon", "Caroline", "Dylan", "Emma", "Gabriel", "Hailey", "Isabel",
                    "Jason", "Kennedy", "Leo", "Megan", "Natalie", "Oscar", "Peyton", "Quinn", "Robert",
                    "Samantha", "Tristan", "Victoria", "Wyatt", "Xenia", "Yolanda", "Zachariah"
            };
            ArrayList<Integer> ids = new ArrayList<>();
            for(int k=0;k<500;k++) {
                ids.add(k);
            }

            String startDateStr = "1990-01-01";
            String endDateStr = "2023-01-01";

            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            Date startDate;
            Date endDate;

            startDate = formatter.parse(startDateStr);
            endDate = formatter.parse(endDateStr);

            for (int i = 0; i < ids.size(); i++) {
                String firstName = names[i % 100];
                String lastName = names[99 - (i % 100)];
                Date randomDate = generateRandomDate(startDate, endDate);

                int id = ids.get( (int) (Math.random() * ids.size()) );
                ids.remove(new Integer(id));
                double gpa = Math.random() * 100;
                String name = firstName + " " + lastName;
                int age = 50;
                if(firstName.equals("Frank") && lastName.equals("Samantha")) {
                    System.out.println("Found frank");
                    randomDate = sdf.parse("2019-01-01");
                    age = 20;
                }

                Hashtable<String, Object> insertedInfo = new Hashtable<>();

                insertedInfo.put("name", name);
                insertedInfo.put("dob", randomDate);
                insertedInfo.put("age", age);
                insertedInfo.put("id", id);
                insertedInfo.put("gpa", gpa);

                dbapp.insertIntoTable("Students", insertedInfo);
            }
            Table tbl = dbapp.getTable("Students");
            tbl.loadindices();

            Hashtable<String, Index> indices = tbl.getindices();
            for (Index index : indices.values()) {
                index.print();
            }
            System.out.println("NUM POINTS");
            System.out.println(dbapp.getTable("Students").toString());
            System.out.println(tbl.toString());
            System.out.println(Point.NUM_POINTS);


            // DELETION
            Hashtable<String, Object> deletedObjects = new Hashtable<>();
            deletedObjects.put("age", 50);
            dbapp.deleteFromTable("Students", deletedObjects);

            // SELECTION

            {

                SQLTerm[] arrSQLTerms = new SQLTerm[3];
                arrSQLTerms[0] = new SQLTerm("Students", "age", "=", 20);
                arrSQLTerms[1] = new SQLTerm("Students", "name", "=", "Frank Samantha");
                arrSQLTerms[2] = new SQLTerm("Students", "dob", "=", sdf.parse("2019-01-01"));
                String[] arrOperators = {"AND", "AND"};

                long t1 = System.nanoTime();
                Iterator it = dbapp.selectFromTable(arrSQLTerms, arrOperators);
                while (it.hasNext()) {
                    Tuple t = (Tuple) it.next();
                    System.out.println(t);
                }

                long t2 = System.nanoTime();


                //// RUN 2
                long t3 = System.nanoTime();

                arrSQLTerms = new SQLTerm[1];
                arrSQLTerms[0] = new SQLTerm("Students", "age", ">=", 50);

                it = dbapp.selectFromTable(arrSQLTerms, new String[0]);
                while (it.hasNext()) {
                    Tuple t = (Tuple) it.next();
                    System.out.println(t);
                }
                long t4 = System.nanoTime();
            }

    }

    // SQL Parser Checking
//    public static void main(String[] args) {
//        DBApp db = new DBApp();
//        db.init();
//        try {
//            db.parseSQL(new StringBuffer("CREATE TABLE omarTable ( id INT, age INT, name VARCHAR(100), dob DATE, gpa DOUBLE )"));
//            db.parseSQL(new StringBuffer("CREATE INDEX akskdk ON omarTable (age, name, dob) USING OCTREE"));
//            db.parseSQL(new StringBuffer("INSERT INTO omarTable (id, age, name, dob, gpa) VALUES (5, 10, 'Omar', '2021-01-01', 5.0)"));
//            db.parseSQL(new StringBuffer("INSERT INTO omarTable (id, age, name, dob, gpa) VALUES (6, 10, 'aaa', '2021-01-01', 5.0)"));
//            db.parseSQL(new StringBuffer("DELETE FROM omarTable WHERE name='Aya'"));
//            db.parseSQL(new StringBuffer("UPDATE omarTable SET name='Ahmed' WHERE id=5"));
//             Iterator t = db.parseSQL(new StringBuffer("SELECT * FROM omarTable WHERE name='Ahmed'"));
//            System.out.println("hola");
//        } catch (DBAppException e) {
//            throw new RuntimeException(e);
//        }
//    }
    CCJSqlParserManager parserManager = new CCJSqlParserManager();

    public Iterator parseSQL( StringBuffer strbufSQL ) throws DBAppException {
        try {
            Statement s = parserManager.parse(new StringReader(strbufSQL.toString()));
            if(s instanceof Select) {
                Select select = (Select) s;
                SelectBody selectBody = select.getSelectBody();
                FromItem fromItem = ((PlainSelect) selectBody).getFromItem();
                String whereClause = ((PlainSelect) selectBody).getWhere().toString();
                String tableName = fromItem.toString();
                List<String> columnNames = new ArrayList<>();

                String[] whereElements = whereClause.split(" ");
                ArrayList<SQLTerm> SQLTerms = new ArrayList<>();

                String sqlTableName = tableName;
                String sqlTableOperator = null;
                String sqlTableColName = null;
                Object sqlTableValue = null;

                ArrayList<String> operators = new ArrayList<>();
                for(String element : whereElements) {
                    if(element.equals("AND") || element.equals("OR") || element.equals("XOR")) {
                        operators.add(element);
                    } else if(element.equals("=") || element.equals(">") || element.equals(">=") || element.equals("<") || element.equals("<=") || element.equals("!=")) {
                        sqlTableOperator = element;
                    } else {
                        if(sqlTableOperator != null) {
                            // this is the value
                            Table tbl = this.getTable(tableName);
                            Hashtable<String, String> colNameTypes = tbl.getColNameType();
                            String type = colNameTypes.get(sqlTableColName);
                            switch(type) {
                                case "java.lang.String":
                                    sqlTableValue = element;
                                    break;
                                case "java.lang.Integer":
                                    sqlTableValue = Integer.parseInt(element);
                                    break;
                                case "java.lang.Double":
                                    sqlTableValue = Double.parseDouble(element);
                                    break;
                                case "java.util.Date":
                                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                                    sqlTableValue = sdf.parse(element.replace("'", ""));
                                    break;
                            }
                            SQLTerm newSQLTerm = new SQLTerm(sqlTableName, sqlTableColName, sqlTableOperator, sqlTableValue);
                            SQLTerms.add(newSQLTerm);
                            System.out.println(newSQLTerm);
                            sqlTableOperator = null;
                        } else {
                            // this is the column name
                            sqlTableColName = element;
                        }
                    }
                }

                SQLTerm[] arrSQLTerms = new SQLTerm[SQLTerms.size()];
                int i = 0;
                for(SQLTerm sss : SQLTerms) {
                    arrSQLTerms[i] = sss;
                    i++;
                }

                String[] arrOperators = new String[operators.size()];
                int j = 0;
                for(String sss : operators) {
                    arrOperators[j] = sss;
                    j++;
                }

                return this.selectFromTable(arrSQLTerms, arrOperators);
            } else if(s instanceof Update) {
                Update update = (Update) s;
                String tableName = update.getTable().getName();
                List<Expression> expressions = update.getExpressions();
                ArrayList<Column> colColumns = update.getUpdateSets().get(0).getColumns();

                ArrayList<String> columns = new ArrayList<>();
                Table t = this.getTable(tableName);


                for(Column col : colColumns) {
                    columns.add(col.getColumnName());
                }

                ArrayList<Expression> columnValues = update.getUpdateSets().get(0).getExpressions();
                Hashtable<String, Object> columnValuesFinal = new Hashtable<>();
                int i=0;
                for(String columnName : columns) {
                    String columnType = t.getColNameType().get(columnName);
                    String value = columnValues.get(i).toString();
                    switch(columnType) {
                        case "java.lang.String":
                            columnValuesFinal.put(columnName, value);
                            break;
                        case "java.lang.Integer":
                            columnValuesFinal.put(columnName, Integer.parseInt(value));
                            break;
                        case "java.lang.Double":
                            columnValuesFinal.put(columnName, Double.parseDouble(value));
                            break;
                        case "java.util.Date":
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                            columnValuesFinal.put(columnName, sdf.parse(value.replace("'", "")));
                            break;
                    }
                }
                String whereClause = update.getWhere().toString();
                String clusteringKeyValueString = whereClause.split(" = ")[1];
                this.updateTable(tableName, clusteringKeyValueString, columnValuesFinal);
            } else if(s instanceof Delete) {
                Delete delObj = (Delete)s;
                String tableName = delObj.getTable().getName();
                String whereClause = delObj.getWhere().toString();


                String[] singleExpressions = whereClause.split("AND");
                ArrayList<String> colvalstrings = new ArrayList<String>();

                for(int i = 0; i < singleExpressions.length; i++) {
                    String temp[] = singleExpressions[i].split("=");
                    for(int j = 0; j < temp.length; j++)
                        colvalstrings.add(temp[j]);
                }



                Hashtable<String,Object> htblcolnamevalue = new Hashtable();
                String colName = null;

                for(int i = 0; i < colvalstrings.size(); i++) {
                    if(i%2==0) {
                        //colname
                        colName = colvalstrings.get(i).trim();
                    }else {
                        //colvalue
                        String value = colvalstrings.get(i).trim();

                        Table tbl = this.getTable(tableName);
                        Hashtable<String, String> colNameTypes = tbl.getColNameType();
                        String type = colNameTypes.get(colName);
                        Object colvalue = null;
                        switch(type) {
                            case "java.lang.String":
                                colvalue = value;
                                break;
                            case "java.lang.Integer":
                                colvalue = Integer.parseInt(value);
                                break;
                            case "java.lang.Double":
                                colvalue = Double.parseDouble(value);
                                break;
                            case "java.util.Date":
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                                colvalue = sdf.parse(value.replace("'", ""));
                                break;
                        }
                        htblcolnamevalue.put(colName, colvalue);

                        colName = null;
                    }
                }



                this.deleteFromTable(tableName, htblcolnamevalue);
                return null;
            } else if(s instanceof Insert) {
                Insert insert = (Insert) s;
                String tableName = insert.getTable().getName();
                List<String> columns = insert.getColumns().stream()
                        .map(column -> column.getColumnName())
                        .collect(Collectors.toList());
                List<Expression> values = ((ExpressionList) insert.getItemsList()).getExpressions();
                Hashtable<String, Object> htblColNameValue = new Hashtable<>();
                Table tbl = this.getTable(tableName);
                Hashtable<String, String> colNameType = tbl.getColNameType();
                int i = 0;
                for(String columnName : columns) {
                    String value = values.get(i).toString();
                    switch(colNameType.get(columnName)) {
                        case "java.lang.String":
                            htblColNameValue.put(columnName, value);
                            break;
                        case "java.lang.Integer":
                            htblColNameValue.put(columnName, Integer.parseInt(value));
                            break;
                        case "java.lang.Double":
                            htblColNameValue.put(columnName, Double.parseDouble(value));
                            break;
                        case "java.util.Date":
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                            htblColNameValue.put(columnName, sdf.parse(value.replace("'", "")));
                            break;
                    }
                    i++;
                }
                this.insertIntoTable(tableName, htblColNameValue);

            } else if(s instanceof CreateTable) {
                CreateTable create = (CreateTable) s;
                String tableName = create.getTable().getName();
                List<ColumnDefinition> columns = create.getColumnDefinitions();
                Hashtable<String, String> colNameType = new Hashtable<>();
                Hashtable<String, String> colNameMin = new Hashtable<>();
                Hashtable<String, String> colNameMax = new Hashtable<>();

                String clusteringKey = null;
                System.out.println("Columns:");

                for (ColumnDefinition column : columns) {
                    String columnName = column.getColumnName();
                    if(clusteringKey == null) clusteringKey = columnName;
                    String dataType = column.getColDataType().getDataType();
                    switch(dataType) {
                        case "INT":
                            colNameType.put(columnName, "java.lang.Integer");
                            colNameMin.put(columnName, "-2147483648");
                            colNameMax.put(columnName, "2147483647");
                            break;
                        case "DOUBLE":
                            colNameType.put(columnName, "java.lang.Double");
                            colNameMin.put(columnName, "-9999999.0");
                            colNameMax.put(columnName, "9999999.0");
                            break;
                        case "VARCHAR":
                            colNameType.put(columnName, "java.lang.String");
                            colNameMin.put(columnName, " ");
                            colNameMax.put(columnName, "zzzzzzzzzzzzzzzzz");
                            break;
                        case "DATE":
                            colNameType.put(columnName, "java.util.Date");
                            colNameMin.put(columnName, "1970-01-01");
                            colNameMax.put(columnName, "2100-01-01");
                            break;
                    }
                    System.out.println(columnName + " " + dataType);
                }
                this.createTable(tableName, clusteringKey, colNameType, colNameMin, colNameMax);

            } else if(s instanceof CreateIndex) {
                // CREATE INDEX
                /* Syntax : 
                    CREATE INDEX ON table_name (column1, column2, column3) USING OCTREE
                */

                String pattern = "CREATE INDEX \\w+ ON (\\w+) \\((\\w+), (\\w+), (\\w+)\\)";
                Pattern regex = Pattern.compile(pattern);

                Matcher matcher = regex.matcher(strbufSQL.toString());

                if (matcher.find()) {
                    String tableName = matcher.group(1);
                    String col1 = matcher.group(2);
                    String col2 = matcher.group(3);
                    String col3 = matcher.group(4);

                    String[] columns = new String[] {col1, col2, col3};

                    this.createIndex(tableName, columns);
                    System.out.println("created index successfully ");
                    return null;
                }
            } else {
                throw new DBAppException("Unsupported SQL");
            }
        } catch(Exception e) {
            e.printStackTrace();

            throw new DBAppException("Invalid SQL Term");
        }
        return null;
    }
}
    