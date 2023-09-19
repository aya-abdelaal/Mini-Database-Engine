package util;


import java.io.File;
import java.io.FileInputStream; 
import java.io.IOException;
 import java.util.Properties; 
 
  public class configReader { 
	  private static final String CONFIG_FILE = "src/main/resources/DBApp.config";
	  private Properties props; 
	  
	  public configReader() throws IOException { 
		  props = new Properties(); 
		  File f = new File(CONFIG_FILE);
		  FileInputStream input = new FileInputStream(f);
		  props.load(input); 
		  } 
	  
	  public int getMaxRowsCount() {
		  return Integer.parseInt(props.getProperty("MaximumRowsCountinTablePage"));
	  }

	  public int getMaxEntriesCount() {
		return Integer.parseInt(props.getProperty("MaximumEntriesinOctreeNode"));
	}
 
//	public String getParameter(String key) { 
//		return props.getProperty(key);
//	}
   
 }
 