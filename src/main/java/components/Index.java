package components;

import jdk.jshell.execution.Util;
import util.Utilities;

import java.io.*;
import java.util.Hashtable;
import java.util.Vector;

public class Index implements Serializable {
    private Node root;
    private String xName;
    private String yName;
    private String zName;
    private String fileName;
    public Index(Comparable minX, Comparable maxX, Comparable minY, Comparable maxY, Comparable minZ, Comparable maxZ, String xName, String yName, String zName, String fileName) {
        this.xName = xName;
        this.yName = yName;
        this.zName = zName;
        this.fileName = fileName;

        try {
            root = new Node(minX, maxX, minY, maxY, minZ, maxZ);
        } catch(Exception e) {
            System.out.println("Unexpected error occured while creating node: " + e.getMessage());
        }
    }

    public void insert(String[] colNames, Comparable[] colValues,
                       Point ref) {
        Comparable[] XYZ = Utilities.getXYZFromColNames(colNames, colValues, xName, yName, zName);

        root.insert(XYZ[0], XYZ[1], XYZ[2], ref);
    }

    public void delete(String[] colNames, Comparable[] colValues, Point ref) {
        Comparable[] XYZ = Utilities.getXYZFromColNames(colNames, colValues, xName, yName, zName);

        root.delete(XYZ[0], XYZ[1], XYZ[2], ref);
    }

    public Vector<Point> search(String[] colNames, String[] operators, Comparable[] colValues) {
        Comparable[] XYZ = Utilities.getXYZFromColNames(colNames, colValues, xName, yName, zName);
        String[] newOperators = Utilities.getOperatorsFromColNames(colNames, operators, xName, yName, zName);
        return root.search(XYZ[0], XYZ[1], XYZ[2], newOperators[0], newOperators[1], newOperators[2]);
    }

    public void print() {
        root.print(1);
    }

    public void update(String[] colNames, Comparable[] oldValues, Comparable[] newValues, Point ref) {
        Comparable[] XYZ_old = Utilities.getXYZFromColNames(colNames, oldValues, xName, yName, zName);
        Comparable[] XYZ_new = Utilities.getXYZFromColNames(colNames, newValues, xName, yName, zName);

        root.delete(XYZ_old[0], XYZ_old[1], XYZ_old[2], ref);
        root.insert(XYZ_new[0], XYZ_new[1], XYZ_new[2], ref);
    }

    public void commit() {
        try {
            FileOutputStream fs = new FileOutputStream(fileName);
            ObjectOutputStream os = new ObjectOutputStream(fs);
            os.writeObject(this);
            os.close();
            fs.close();
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public String[] getColumnNames() {
        return new String[] {xName, yName, zName};
    }
}
