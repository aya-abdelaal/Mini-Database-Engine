package components;

import java.util.*;

import Exceptions.DBAppException;
import util.Utilities;

import java.io.*;
import java.text.ParseException;

public class Page implements Serializable {
	private static final long serialVersionUID = 1L;
	private static int N;

	private int pageNumber;
	private Vector<Tuple> records;

	// acts as loadPage, deserializing
	public Page(int pageNumber, String tableName) {
		this.pageNumber = pageNumber;
		try {
			// Generate a unique filename using the page number
			String fileName = "src/main/resources/data/page_" + tableName + pageNumber + ".ser";
			// Initialize input stream
			FileInputStream fInputStream = new FileInputStream(fileName);
			ObjectInputStream inputStream = new ObjectInputStream(fInputStream);
			// Read the vector from the file
			this.records = (Vector<Tuple>) inputStream.readObject();
			inputStream.close();

		} catch (IOException e) {
			System.out.println("error in loading page" + e.getMessage());
		} catch (ClassNotFoundException e) {
			System.out.println("trying to load page that doesn't exist" + e.getMessage());
		}
	}

	// used to initialize values in new page
	private Page(int pageNumber) {
		this.pageNumber = pageNumber;
		records = new Vector<Tuple>();
	}

	public static Page createPage(int pageNumber, String tableName) {
		String fileName = "src/main/resources/data/page_" + tableName + pageNumber + ".ser";
		File f = new File(fileName);
		try {
			f.createNewFile();
		} catch (IOException e) {
			System.out.println("error when creating new page file" + e.getMessage());
		}
		Page page = new Page(pageNumber);
		page.commit(tableName);
		return page;
	}

	// serializing vector
	public void commit(String tableName) {
		try {
			FileOutputStream fOutputStream = new FileOutputStream("src/main/resources/data/page_" + tableName + this.pageNumber + ".ser");

			ObjectOutputStream outputStream = new ObjectOutputStream(fOutputStream);
			outputStream.reset();
			outputStream.writeObject(this.records);
			outputStream.close();
		} catch (IOException e) {
			System.out.println("Error writing page to file 1: " + e.getMessage());
		}
	}

	public int getPageNumber() {
		return pageNumber;
	}

	public Vector<Tuple> getrecords() {
		return records;
	}

	public boolean isEmpty() {
		return records.size() == 0;
	}

	public boolean isFull() {
		return records.size() == N;
	}

	// use if we don't have PK
	public ArrayList<Tuple> deleteTuples(Hashtable<String, Object> htblColNameValue, Hashtable<String, String> colNameType,
			String tableName) throws DBAppException {
		ArrayList<Tuple> deletedTuples = new ArrayList<Tuple>();
			int j=0;
		for (int i = 0; i < records.size(); i++) {
			Enumeration<String> colNames = htblColNameValue.keys();
			boolean flag = true;
			Tuple tuple = null;

			// check columns on this record
			while (colNames.hasMoreElements()) {

				tuple = records.get(i);
				String colName = colNames.nextElement();
				
				if(colNameType.get(colName)==null)
					throw new DBAppException("column does not exist");
				else {
				if (colNameType.get(colName).equals("java.lang.Date")) {
					
					if(htblColNameValue.get(colName)==null)
						throw new DBAppException("column does not exist");
					else {
					if (Utilities.compareToDates(tuple.getValue(colName),
							htblColNameValue.get(colName)) != 0) {
						flag = false;
						break;
					}}
				} else {
					if(htblColNameValue.get(colName)==null)
						throw new DBAppException("column does not exist");
					
					if (!(htblColNameValue.get(colName).equals(tuple.getValue(colName)))) {
						flag = false;
						break;
					}

				}}
			}

			if (flag) {
				records.remove(i);
				deletedTuples.add(tuple);
				j++;
				i--;
			}

		}

		this.commit(tableName);
		return deletedTuples;
	}

	// use if we have PK
	public Tuple deleteTuple(int index, String tableName) {
		Tuple tuple = records.get(index);
		records.remove(index);
		this.commit(tableName);
		return tuple;
	}

	public int findTuple(String clusteringKey, boolean clusteringKeyComparable, Object clusteringKeyValue) throws DBAppException {
		// binary search in page to find tuple
		int beg = 0;
		int end = records.size() - 1;
		while (beg <= end) {
			int middle = (beg + end) / 2;
			Object recordClusteringKeyValue = records.get(middle).getValue(clusteringKey);
			
			int compareValue;
			if (clusteringKeyComparable)
				compareValue = myCompareTo(clusteringKeyValue,recordClusteringKeyValue);

			else
				compareValue = Utilities.compareToDates(clusteringKeyValue, recordClusteringKeyValue);

			if (compareValue == 0)
				return middle;
			else if (compareValue < 0)
				end = middle - 1;
			else
				beg = middle + 1;
		}
		return -1;
	}

	public void updateTuple(int index, Hashtable<String, Object> htblColNameValue, String tableName) {
		Tuple record = records.get(index);
		Enumeration<String> colNames = htblColNameValue.keys();
		while (colNames.hasMoreElements()) {
			String colName = colNames.nextElement();
			record.updateValue(colName, htblColNameValue.get(colName));
		}
		this.commit(tableName);
	}

	/** ----- code for insert + helpers ----- **/

	// return null if no overflow, return extra tuple if overflow occurs
	public Tuple insertTuple(Hashtable<String, Object> htblColNameValue, String tableName, String clusteringKey,
			boolean isClusteringKeyComparable2, Point point) throws DBAppException {
		boolean isClusteringKeyComparable = true;
		Object clusteringKeyValue = htblColNameValue.get(clusteringKey);

		// if page empty
		if (records.size() == 0) {
			addTuple(htblColNameValue, 0, clusteringKeyValue, point);
			this.commit(tableName);
			return null;
		} else {
			int index = getTuplePosition(clusteringKey, htblColNameValue.get(clusteringKey),
					isClusteringKeyComparable);
			if (index == -1)
				throw new DBAppException("key is not unique");

			if (addTuple(htblColNameValue, index, clusteringKeyValue, point)) {
				Tuple tuple = records.remove(records.size() - 1);
				this.commit(tableName);
				return tuple;
			} else {
				this.commit(tableName);
				return null;
			}
		}
	}

	// for directly inserting in the beginning without searching
	public Tuple insertTupleFirst(Hashtable<String, Object> htblColNameValue, String tableName, Object clusteringKey, Point point) {
		// Point point = new Point(this.getPageNumber(), clusteringKey);
		Tuple tuple = new Tuple(htblColNameValue, point);
		records.add(0, tuple);
		if (records.size() > N) {
			Tuple tuple1 = records.remove(records.size() - 1);
			this.commit(tableName);
			return tuple1;
		}
		this.commit(tableName);
		return null;
	}

	public void appendTuple(Hashtable<String, Object> htblColNameValue, String tableName, Object clusteringKey, Point point) {
		// Point point = new Point (this.getPageNumber(), clusteringKey);
		Tuple tuple = new Tuple(htblColNameValue, point);
		records.add(tuple);
		this.commit(tableName);
	}

	// performs binary search variation to attempt to find tuple position
	private int getTuplePosition(String clusteringKey, Object clusteringKeyValue, boolean clusteringKeyComparable) throws DBAppException {
		int beg = 0;
		int end = records.size() - 1;
		while (beg <= end) {
			int middle = (beg + end) / 2;
			Object midClusteringKeyValue =  records.get(middle).getValue(clusteringKey);
			
			Object preClusteringKeyValue;
			if(middle != 0)
				preClusteringKeyValue =  records.get(middle - 1).getValue(clusteringKey);
			else
				preClusteringKeyValue =  records.get(middle).getValue(clusteringKey);
			int midCompareValue;
			int preCompareValue;

			if (clusteringKeyComparable) {
				midCompareValue = myCompareTo(clusteringKeyValue,midClusteringKeyValue);
				if (middle != 0)
					preCompareValue = myCompareTo(clusteringKeyValue,preClusteringKeyValue);
				else
					preCompareValue = 1;
			}

			else {
				midCompareValue = Utilities.compareToDates(clusteringKeyValue, midClusteringKeyValue);
				if (middle != 0)
					preCompareValue = Utilities.compareToDates(clusteringKeyValue, preClusteringKeyValue);
				preCompareValue = 1;
			}

			if (midCompareValue < 0 && preCompareValue > 0)
				return middle;
			else if (preCompareValue < 0)
				end = middle - 1;
			else
				beg = middle + 1;

		}
		if (beg == records.size())
			return beg;
		return -1;
	}

	public Tuple insertOverflowTuple(Tuple tuple, String tableName) {
		records.add(0, tuple);
		this.commit(tableName);
		if (records.size() > N) {
			Tuple tuple1 = records.remove(records.size() - 1);
			this.commit(tableName);
			return tuple1;
		} 
		return null;
	}

	// returns true if overflow occurs
	private boolean addTuple(Hashtable<String, Object> htblcolnamevalue, int index, Object clusteringKey, Point point) {
		Tuple tuple = new Tuple(htblcolnamevalue, point);
		records.add(index, tuple);
		return (records.size() > N);
	}

	public boolean checkPageRange(String clusteringKey, Object strClusteringKeyValue, boolean clusteringKeyComparable) throws DBAppException {
		Object minValue =  records.get(0).getValue(clusteringKey);
		Object maxValue = records.get(records.size() - 1).getValue(clusteringKey);

		if (clusteringKeyComparable) {
				return (myCompareTo(strClusteringKeyValue,minValue) >= 0)
						&& (myCompareTo(strClusteringKeyValue,maxValue) <= 0);

		} else {
			return Utilities.compareToDates(strClusteringKeyValue, minValue) >= 0

					&& Utilities.compareToDates(strClusteringKeyValue, maxValue) <= 0;
		}
	}

	private int myCompareTo(Object strClusteringKeyValue, Object minValue) {
		try {
			Integer x = (int) strClusteringKeyValue;
			Integer y = (int)minValue;
			return x.compareTo(y);
		} catch (ClassCastException e) {

		}
		
		try {
		Double x = (double)strClusteringKeyValue;
		Double y = (double)minValue;
		return x.compareTo(y);
		}catch(ClassCastException e) {
			
		}
		
		if (strClusteringKeyValue instanceof String)
			return ((strClusteringKeyValue+"").compareTo(minValue+""));
		return 0;

	}

	public boolean checkLessThanMin(String clusteringKey, Object clusteringKeyValue, boolean clusteringKeyComparable) throws DBAppException {
		Object minValue = records.get(0).getValue(clusteringKey);

		if (clusteringKeyComparable) {

			try {
				int x = (int) clusteringKeyValue;
				int y =(int) minValue;
				return (x < y);
			} catch (ClassCastException e) {

			}
			
			try {
			double x = (double)clusteringKeyValue;
			double y = (double)minValue;
			return ((x < y));
			}catch(ClassCastException e) {
				
			}
			
			if (clusteringKeyValue instanceof String)
				return ((((String) clusteringKeyValue).compareTo((String) minValue) < 0));

		} else {
			return ((Utilities.compareToDates(clusteringKeyValue, minValue) < 0));
		}
		return false;
	}

	// do it for SQLTerms and select
	public ArrayList<Tuple> findSQLResultInPage(String colName, Comparable colValue, String operator) {
		ArrayList<Tuple> resultSet = new ArrayList<>();
		for (int i = 0; i < records.size(); i++) {
			Tuple record = records.get(i);
			Comparable colValueExpected = (Comparable) record.getAttribute(colName);
			boolean add = switch (operator) {
				case "=" -> colValueExpected.equals(colValue);
				case "!=" -> !colValueExpected.equals(colValue);
				case ">" -> colValueExpected.compareTo(colValue) > 0;
				case ">=" -> colValueExpected.compareTo(colValue) > 0 || colValueExpected.equals(colValue);
				case "<" -> colValueExpected.compareTo(colValue) < 0;
				case "<=" -> colValueExpected.compareTo(colValue) < 0 || colValueExpected.equals(colValue);
				default -> false;
			};
			if(add) {
				resultSet.add(record);
			}
		}
		return resultSet;

	}

	public static void setMaximumRows(int max) {
		N = max;
	}

	public String toString() {
		return records.toString();
	}

}
