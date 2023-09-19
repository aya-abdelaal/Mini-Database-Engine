package components;

import java.io.*;

import util.Utilities;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

import Exceptions.DBAppException;

public class Table implements Serializable {
	private static final long serialVersionUID = 1L;
	private String tableName;
	private String clusteringKey;
	private Hashtable<String, String> colNameType;
	private Hashtable<String, String> colNameMin;
	private Hashtable<String, String> colNameMax;
	private Vector<Integer> pages;
	private boolean clusteringKeyComparable;
	private int counter;

	private Hashtable<String, Index> indices = new Hashtable<String, Index>();

	public Table(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) {
		this.tableName = tableName;
		this.clusteringKey = clusteringKey;
		this.colNameType = colNameType;
		this.colNameMin = colNameMin;
		this.colNameMax = colNameMax;
		this.pages = new Vector<>();
		counter = 0;
		
		File dir = new File("src/main/resources/data/table");
		if (!dir.exists()) {
			dir.mkdir();
		}

		File f = new File("src/main/resources/data/table/"+ tableName+"_table.ser");
		
		clusteringKeyComparable = Utilities.getColumnTypeComparable(tableName, clusteringKey);
		
		try {
			f.createNewFile();
			this.commit();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public Hashtable<String, Index> getindices() {
		return this.indices;
	}

	public void loadindices() {
		try {
			BufferedReader reader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			String line = reader.readLine();
			while(line != null) {
				String[] lineDetails = line.split(",");

				line = reader.readLine();

				if(!lineDetails[0].equals(tableName)) {
					continue;
				}

				String colName = lineDetails[1];
				String indexName = lineDetails[4];
				if(indexName.equals("null")) {
					continue;
				}
				String indexType = lineDetails[5];
				if(indexType.equals("octree")) {
					if(!this.indices.containsKey(indexName)) {
						this.getIndex(indexName);
					}
				}
			}
		} catch(Exception e) {
			System.out.println(e.getMessage());
		}
	}
	public void unloadindices() {
		this.indices = new Hashtable<>();
	}
	public String getTableName() {
		return tableName;
	}

	public String getClusteringKey() {
		return clusteringKey;
	}

	public Hashtable<String, String> getColNameType() {
		return colNameType;
	}

	public Hashtable<String, String> getColNameMin() {
		return colNameMin;
	}

	public Hashtable<String, String> getColNameMax() {
		return colNameMax;
	}

	public Vector<Integer> getPages() {
		return pages;
	}

	public void commit() {
		try {
			FileOutputStream fs = new FileOutputStream("src/main/resources/data/table/"+ tableName+"_table.ser");
			ObjectOutputStream out = new ObjectOutputStream(fs);
			out.writeObject(this);
			out.flush();
			out.close();
		} catch (IOException e) {
			//
			e.printStackTrace();
		}
	}

	// create the page then add it to the table
	private Page addPage() {
		Page page = Page.createPage(counter, tableName);
		pages.add(counter++);
		return page;
	}

	private Index getIndex(String indexName) {
		if(indices.containsKey(indexName)) {
			return indices.get(indexName);
		} else {
			FileInputStream fs;
			try {
				String indexFileName = 	"src/main/resources/data/index_" + tableName + indexName + ".ser";

				fs = new FileInputStream(indexFileName);
				ObjectInputStream in = new ObjectInputStream(fs);

				Index index = (Index) in.readObject();
				indices.put(indexName, index);
				in.close();
				return index;
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
	}

	public void removePage(Page page) {
		// Generate the filename to be deleted
		String filename = "src/main/resources/data/page_" + tableName + page.getPageNumber() + ".ser";

		// Create a File object with the filename
		File fileToDelete = new File(filename);

		// Check if the file exists
		if (fileToDelete.exists()) {
			// Attempt to delete the file
			if (fileToDelete.delete()) {
				System.out.println("File deleted successfully.");
			} else {
				System.out.println("Failed to delete the file.");
			}
		} else {
			System.out.println("File does not exist.");
		}
		
		
		pages.removeElement(page.getPageNumber()); // remove page number
	}

	public void insert(Hashtable<String, Object> htblcolnamevalue) throws DBAppException {
		this.loadindices();
		Page page = null;
		Point point = null;
		if(!validateData(htblcolnamevalue, true)) {
			//throw new DBAppException("Invalid data");
			return;
		}
		int insertedInPage = -1;
		Object clusteringKeyValue = htblcolnamevalue.get(clusteringKey);
		// check if the table is empty -> create page and insert
		if (pages.size() == 0) {
			page = addPage();
			point = new Point(page.getPageNumber(), clusteringKeyValue);
			page.insertTupleFirst(htblcolnamevalue, tableName, clusteringKeyValue, point);
			insertedInPage = page.getPageNumber();

		}
		// else linear search on pages and insert in right page
		else {
			Tuple overflowTuple = null;
			Page previousPage = null;

			// find the page I need to insert in
			int i = 0;
			for (i = 0; i < pages.size(); i++) {
				int pageNumber = pages.get(i);
				page = new Page(pageNumber, tableName);

				// 3 cases
				// 1) within page range -> check if PK exists, if not -> insert in the middle of
				// page
				if (page.checkPageRange(clusteringKey, clusteringKeyValue, clusteringKeyComparable)) {
					if (isUnique(clusteringKeyValue, page)) {
						insertedInPage = pageNumber;
						point = new Point(page.getPageNumber(), clusteringKeyValue);
						overflowTuple = page.insertTuple(htblcolnamevalue, tableName, clusteringKey,
								clusteringKeyComparable, point);
						break;
					} else
						throw new DBAppException("Inserted PK is not unique");
				}
				// 2) less than page minimum -> insert in the page before
				// if page before is full or first page -> insert in curr page
				if (page.checkLessThanMin(clusteringKey, clusteringKeyValue, clusteringKeyComparable)) {
					if (pageNumber != pages.get(0) && !previousPage.isFull()) {
						point = new Point(previousPage.getPageNumber(), clusteringKeyValue);
						previousPage.appendTuple(htblcolnamevalue, tableName, clusteringKeyValue, point);
						insertedInPage = pageNumber - 1;
					} else {
						point = new Point(page.getPageNumber(), clusteringKeyValue);
						overflowTuple = page.insertTupleFirst(htblcolnamevalue, tableName, clusteringKeyValue, point);
						insertedInPage = pageNumber;
					}
					break;
				}
				// 3) greater than max of page -> go to next page
				previousPage = page;

			}

			// once i have page number in insertedInPage -> insert and handle overflow

			// if -1 -> greater than max of last page -> either insert in last page or
			// create new page
			if (insertedInPage == -1) {
				if (page.isFull()) {
					Page newPage = addPage();
					point = new Point(newPage.getPageNumber(), clusteringKeyValue);
					newPage.insertTupleFirst(htblcolnamevalue, tableName, clusteringKeyValue, point);
					page = newPage;
				} else {
					point = new Point(page.getPageNumber(), clusteringKeyValue);
					page.appendTuple(htblcolnamevalue, tableName, clusteringKeyValue, point);
				}
				insertedInPage = page.getPageNumber();
			} else {
				// handle overflow
				i++;
				while (overflowTuple != null) {
					// if we ran out of pages -> create new page
					if (i == pages.size())
						page = addPage();
					else {
						page = new Page(pages.get(i), tableName);
						i++;
					}
					//overflowTuple.getPoint().setPage(page.getPageNumber()); // SHIFTING INDEX
					for(Object objIndex : this.getindices().values()) {
						Index index = (Index) objIndex;
						String[] colNames = index.getColumnNames();
						Comparable[] values = new Comparable[3];
						for(int j=0;j<values.length;j++) {
							values[j] = (Comparable) overflowTuple.getAttribute(colNames[j]);
						}
						Vector<Point> searchResults = index.search(colNames, new String[] {"=", "=", "="}, values);
						if(searchResults.size() >= 1) {
							Point pt = searchResults.get(0);
							while(pt != null) {
								if(pt.getId().equals(overflowTuple.getAttribute(this.getClusteringKey()))) {
									pt.setPage(page.getPageNumber());
									break;
								}
								pt = pt.getPoint();
							}
						}
					}
					overflowTuple = page.insertOverflowTuple(overflowTuple, tableName);
				}

			}
		}

		for(String indexName : indices.keySet()) {
			Index currIndex = indices.get(indexName);
			String[] colNames = currIndex.getColumnNames();
			Point entryPoint = point;
			currIndex.insert(colNames, new Comparable[] { (Comparable) htblcolnamevalue.get(colNames[0]), (Comparable) htblcolnamevalue.get(colNames[1]), (Comparable) htblcolnamevalue.get(colNames[2]) }, entryPoint);
			currIndex.commit();
		}
		this.unloadindices();
		this.commit();
	}

	private boolean isUnique(Object clusteringKeyValue, Page page) throws DBAppException {
		return (page.findTuple(clusteringKey, clusteringKeyComparable, clusteringKeyValue) == -1);
	}

	public boolean validateData (Hashtable<String,Object> htblcolnamevalue, boolean flag) throws DBAppException{
        String strTableName = this.tableName;
        
        if(!flag && htblcolnamevalue.size() > colNameType.size() - 1) {
        	throw new DBAppException("extra columns in update");}
        else if(flag && htblcolnamevalue.size() > colNameType.size()) {
			throw new DBAppException("extra columns in insert");
		}
            try {
            	int i = 0;
                BufferedReader reader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
                String line;
                Set<String> keys = htblcolnamevalue.keySet();
                while((line = reader.readLine()) != null){
                	if (line.startsWith(strTableName + ",")) {
                		String[] metadatastr = line.split(",");
                		String key = metadatastr[1];
                		
                		if(htblcolnamevalue.containsKey(key)) {
                			i++;
                			if(!flag && metadatastr[3].equals("TRUE")) {
                				throw new DBAppException("can't update the PK");
                			}
                			Object value = htblcolnamevalue.get(key);
                			//check correct type
    
                			if(!colNameType.get(key).equals(value.getClass().getName())) {
                				throw new DBAppException("invalid insert type "+ value + "   " +value.getClass().getName()+" and "+colNameType.get(key));}
                			String minString = colNameMin.get(key);
                			String maxString = colNameMax.get(key);
                			
                			Object min;
                			Object max;
                			
                			switch(metadatastr[2]) {
                			case "java.lang.Integer" : min = Integer.parseInt(minString);
                			max = Integer.parseInt(maxString);
                			break;
                			case "java.lang.Double" : min = Double.parseDouble(minString);
                				max = Double.parseDouble(maxString);
                				break;
                			case  "java.util.Date" :
								minString = minString.replace('-', '/');
								maxString = maxString.replace('-', '/');
								min = new SimpleDateFormat("yyyy/MM/dd").parse(minString);
                				max = new SimpleDateFormat("yyyy/MM/dd").parse(maxString);
                			break;
                			default: min = minString;
                			max = maxString;
                			}
                			
                			
                			if( (((Comparable) value).compareTo(min) < 0 || ((Comparable) value).compareTo(max) > 0)) {
								int cv = ((Comparable) value).compareTo(max);
                				throw new DBAppException("Inserted column " + key + " is not within range");
                				//return false;
                				}
                			
                		}
                		else {
                			if(flag && metadatastr[3].equals("TRUE"))
                				throw new DBAppException("Inserted tuple should have a PK");
                		}
                	}
                }
                if(i != htblcolnamevalue.size())
                	throw new DBAppException("Inserting column not in table");
                reader.close();

                }
             catch (IOException e) {
                throw new DBAppException("An error occurred while reading the metadata file.");
            } catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("error in validating insert while parsing dates");
			}
            
            return true;
    }

	public void initIndex(String indexName, String[] strArrColName) {
		try {
			String xName = strArrColName[0];
			String yName = strArrColName[1];
			String zName = strArrColName[2];

			String xType = colNameType.get(xName);
			String yType = colNameType.get(yName);
			String zType = colNameType.get(zName);

			String minX = colNameMin.get(xName);
			String minY = colNameMin.get(yName);
			String minZ = colNameMin.get(zName);

			String maxX = colNameMax.get(xName);
			String maxY = colNameMax.get(yName);
			String maxZ = colNameMax.get(zName);

			Comparable minXC = null, maxXC = null, minYC = null, maxYC = null, minZC = null, maxZC = null;

			for(int i=0;i<3;i++) {
				String curType = i == 0 ? xType : (i == 1 ? yType : zType);

				String currMin = i == 0 ? minX : (i == 1 ? minY : minZ);
				String currMax = i == 0 ? maxX : (i == 1 ? maxY : maxZ);
				Comparable minCurrType;
				Comparable maxCurrType;

				switch (curType) {
					case "java.lang.Integer":
						minCurrType = Integer.parseInt(currMin);
						maxCurrType = Integer.parseInt(currMax);
						break;
					case "java.lang.Double":
						minCurrType = Double.parseDouble(currMin);
						maxCurrType = Double.parseDouble(currMax);
						break;
					case "java.util.Date":
						minCurrType = new SimpleDateFormat("yyyy-MM-dd").parse(currMin);
						maxCurrType = new SimpleDateFormat("yyyy-MM-dd").parse(currMax);
						break;
					default:
						minCurrType = currMin;
						maxCurrType = currMax;
						break;
				}

				if (i == 0) {
					minXC = minCurrType;
					maxXC = maxCurrType;
				} else if (i == 1) {
					minYC = minCurrType;
					maxYC = maxCurrType;
				} else {
					minZC = minCurrType;
					maxZC = maxCurrType;
				}
			}


			String indexFileName = "index_" + tableName + indexName;
			String fileName = "src/main/resources/data/" + indexFileName + ".ser";
			Index newIndex = new Index(minXC, maxXC, minYC, maxYC, minZC, maxZC, xName, yName, zName, fileName);

			File f = new File(fileName);

			try {
				f.createNewFile();
			} catch (IOException e) {
				System.out.println("error when creating new page file" + e.getMessage());
			}



			try {
				FileOutputStream fOutputStream = new FileOutputStream(fileName);

				ObjectOutputStream outputStream = new ObjectOutputStream(fOutputStream);
				outputStream.reset();
				outputStream.writeObject(newIndex);
				outputStream.close();
				fOutputStream.close();
			} catch (IOException e) {
				System.out.println("Error writing page to file 2: " + e.getMessage());
			}

			File metadataFile = new File("src/main/resources/metadata.csv");
			BufferedReader fr = new BufferedReader(new FileReader(metadataFile));
			String line;
			StringBuilder sb = new StringBuilder();
			while((line = fr.readLine()) != null) {
				String[] lineArr = line.split(",");
				String tName = lineArr[0];
				String colName = lineArr[1];

				if (tName.equals(tableName)) {
					if(colName.equals(xName) || colName.equals(yName) || colName.equals(zName)) {
						lineArr[4] = indexName;
						lineArr[5] = "octree";
						sb.append(String.join(",", lineArr));
						sb.append('\n');
					} else {
						sb.append(line);
						sb.append('\n');
					}
				} else {
					sb.append(line);
					sb.append('\n');
				}
			}
			fr.close();

			FileWriter fw = new FileWriter(metadataFile);
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(sb.toString());
			bw.flush();
			bw.close();
			fw.close();

		} catch(Exception e) {
			System.out.println("Error writing page to file 3: " + e.getMessage());
		}
	}
	public void update(String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		
		validateData(htblColNameValue, false);
		
		if(htblColNameValue.containsKey(strClusteringKeyValue))
			throw new DBAppException("You can't update the PK");
		
		// linear search on pages
		Page page;

		for (int page_num : pages) {
			// load page
			page = new Page(page_num, tableName);

			// check if value within range
			if (page.checkPageRange(clusteringKey, strClusteringKeyValue, clusteringKeyComparable)) {
				// binary search and update

				int index = -1;
				index = page.findTuple(clusteringKey, clusteringKeyComparable, strClusteringKeyValue);
				if (index != -1) {
					Tuple tupleInstance = page.getrecords().get(index);

					this.loadindices();
					Hashtable<String, Index> indices = this.getindices();
					Hashtable<Index, Comparable[]> oldValuesForEachIndex = new Hashtable<>();
					for(Index idx : indices.values()) {
						String[] colNames = idx.getColumnNames();
						Comparable oldValueX = (Comparable) tupleInstance.getAttribute(colNames[0]);
						Comparable oldValueY = (Comparable) tupleInstance.getAttribute(colNames[1]);
						Comparable oldValueZ = (Comparable) tupleInstance.getAttribute(colNames[2]);

						Comparable[] oldValues = {oldValueX, oldValueY, oldValueZ};
						oldValuesForEachIndex.put(idx, oldValues);
					}
					page.updateTuple(index, htblColNameValue, this.tableName);
					for(Index idx : oldValuesForEachIndex.keySet()) {
						String[] colNames = idx.getColumnNames();
						Comparable newValueX = (Comparable) tupleInstance.getAttribute(colNames[0]);
						Comparable newValueY = (Comparable) tupleInstance.getAttribute(colNames[1]);
						Comparable newValueZ = (Comparable) tupleInstance.getAttribute(colNames[2]);

						idx.update(colNames, oldValuesForEachIndex.get(idx), new Comparable[] {newValueX, newValueY, newValueZ}, tupleInstance.getPoint());
						idx.commit();
					}
				}

				break;
			} else
				continue;
		}

	}
	
	public void validateDelete(Hashtable<String, Object> htblColNameValue) throws DBAppException {
		if(htblColNameValue.size() > colNameType.size())
			throw new DBAppException("Extra columns in delete");
	}

	public ArrayList <Tuple> delete(Hashtable<String, Object> htblColNameValue) throws DBAppException {
		validateDelete(htblColNameValue);
		ArrayList <Tuple> deletedTuples =new ArrayList<Tuple>();
		// check if we have the clustering key value (for binary search)
		Enumeration<String> colNames = htblColNameValue.keys();
		boolean flagClusteringKey = false;
		while (colNames.hasMoreElements()) {
			if (colNames.nextElement().equals(clusteringKey)) {
				flagClusteringKey = true;
				break;
			}
		}

		if (flagClusteringKey) {

			// binary search
			Page page = null;
			if(htblColNameValue.get(clusteringKey)==null)
				throw new DBAppException("clustering key does not exist");
			else {
				Object strClusteringKeyValue = htblColNameValue.get(clusteringKey);
				for (int i = 0; i < pages.size(); i++) {
					int page_num = pages.get(i);

					page = new Page(page_num, tableName);
					// check if value within range
					if (page.checkPageRange(clusteringKey, strClusteringKeyValue, clusteringKeyComparable)) {
						// binary search and update
						int index = page.findTuple(clusteringKey, clusteringKeyComparable, strClusteringKeyValue);


						if (index != -1) {
							Tuple tuple = page.deleteTuple(index, tableName);
							deletedTuples.add(tuple);

							break;
						}
						else{
						throw new DBAppException("Tuple does not exit");
						}
					}
				}
			}
		} else {
			// linear search on entire table
			for (int i = 0; i < pages.size(); i++) {

				int page_num = pages.get(i);

				// load page
				Page page = null;
				page = new Page(page_num, tableName);

				ArrayList<Tuple> deletedTuplesPerPage = page.deleteTuples(htblColNameValue, colNameType, this.tableName);
				deletedTuples.addAll(deletedTuplesPerPage);
				if (page.isEmpty()) {
					removePage(page);
					i--;
				}
			}

		}
		// Have to find POINT first, maybe use .search on node with exact value to get it
//		this.loadindices();
//		for(String indexName : indices.keySet()) {
//			Index currIndex = indices.get(indexName);
//			String[] colNames2 = currIndex.getColumnNames();
//			currIndex.delete(colNames2, new Comparable[] { (Comparable) htblColNameValue.get(colNames2[0]), (Comparable) htblColNameValue.get(colNames2[1]), (Comparable) htblColNameValue.get(colNames2[2]) });
//			currIndex.commit();
//		}
//		this.unloadindices();
		this.commit();
		return deletedTuples;
	}

	// ml2
	// for SQL terms
	public ArrayList<Tuple> findInTable(String colName, Comparable colValue, String operator) {

		// hashtable saving page,row of results
		ArrayList<Tuple> resultSet = new ArrayList<>();

		for (int i = 0; i < pages.size(); i++) {

			int page_num = pages.get(i);

			// load page
			Page page = null;
			page = new Page(page_num, tableName);

			// find matching rows in page
			ArrayList<Tuple> pageRows = page.findSQLResultInPage(colName, colValue, operator);

			resultSet.addAll(pageRows);
		}

		return resultSet;
	}

	public String toString(int num) {
		Page p = null;
		p = new Page(num, tableName);
		return p.toString();

	}
	
	public String toString() {
		StringBuilder pagesS = new StringBuilder();
		for(int i=0;i<this.pages.size();i++) {
			Page objPage = new Page(this.pages.get(i), this.tableName);
			pagesS.append("PAGE " + i + "\n");
			pagesS.append( objPage.toString() + "\n");
		}
		return pages.toString();
	}
	

}