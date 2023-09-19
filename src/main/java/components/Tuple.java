package components;
import java.io.Serializable;
import java.util.*;

public class Tuple implements Serializable {

    private Hashtable<String, Object> tupleData;
    private Point point;
    public Tuple(Hashtable<String, Object> tupleDataComing, Point point) {

        tupleData = tupleDataComing;
        this.point =point;
    }

    public void addAttribute(String colName, String value) {
        tupleData.put(colName, value);
    }

    public Object getAttribute(String colName) {
        return tupleData.get(colName);
    }

    public Point getPoint(){
        return this.point;
    }

    public Hashtable<String, Object> getTupleData() {
        return tupleData;
    }

    public void setTupleData(Hashtable<String, Object> tupleData) {
        this.tupleData = tupleData;
    }
    
    public Object getValue(String Key) {
    	return tupleData.get(Key);
    }
    
    public void updateValue(String Key, Object object) {
    	tupleData.put(Key, object);
    }
    
    public String toString() {
    	String str = "";
    	 for (String key : tupleData.keySet()) {
             String value = (String) tupleData.get(key).toString();
             str += value + " , ";
    	 }
    	 str += "\n";
    	return str;
    }

    @Override
    public boolean equals(Object o) {
        Tuple t = (Tuple) o;
        for(String col : tupleData.keySet()) {
            if(!tupleData.get(col).equals(t.getValue(col))) {
                return false;
            }
        }
        return true;
    }
}

