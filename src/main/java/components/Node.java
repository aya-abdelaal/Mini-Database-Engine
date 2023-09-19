package components;
import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

import util.Utilities;
import util.configReader;

public class Node implements Serializable {
    private int MAX_POINTS_PER_NODE = new configReader().getMaxEntriesCount();;

    private Node[] childNodes;
    private Comparable minX;
    private Comparable maxX;
    private Comparable minY;
    private Comparable maxY;
    private Comparable minZ;
    private Comparable maxZ;

    private Vector<Comparable[]>  values = null;
    private Vector<Point> refs = null;
    private boolean empty;
    private boolean leaf;

    public Node(Comparable minX, Comparable maxX, Comparable minY, Comparable maxY, Comparable minZ, Comparable maxZ) throws IOException {
        this.minX = minX;
        this.minY = minY;
        this.minZ = minZ;
        this.maxX = maxX;
        this.maxY = maxY;
        this.maxZ = maxZ;

        childNodes = new Node[8];
        empty = true;
        leaf = true;
        this.values = new Vector(MAX_POINTS_PER_NODE);
        this.refs = new Vector(MAX_POINTS_PER_NODE);
    }

    public void print(int i) {
        if(leaf) {
            if(empty) {
                for(int j=0;j<i;j++) {
                    System.out.print("   ");
                }
                System.out.println("LEAF EMPTY");
                return;
            }
            this.printValues(i);
        } else {
            for(int j=0;j<i;j++) {
                System.out.print("   ");
            }
            System.out.println("NON-LEAF X: [" + minX + ", " + maxX + "], Y: [" + minY + ", " + maxY + "], Z: [" + minZ + ", " + maxZ + "]");
            i++;
            for(Node childNode : childNodes) {
                childNode.print(i);
            }
        }
    }

    private void printValues(int i) {
        for(int j=0;j<i;j++) {
            System.out.print("   ");
        }
        System.out.print("LEAF (");
        for(Comparable[] value : values) {
            System.out.print("[" + value[0] + " | " + value[1] + " | " + value[2] + "] /// ");
        }
        System.out.println(")");
    }

    private boolean belongs(Comparable x, Comparable y, Comparable z, String operatorX, String operatorY, String operatorZ) {
        boolean xComparison = false;
        boolean yComparison = false;
        boolean zComparison = false;

        xComparison = switch (operatorX) {
            case "<" -> x.compareTo(this.maxX) < 0;
            case "<=" -> x.compareTo(this.maxX) <= 0;
            case ">" -> x.compareTo(this.minX) > 0;
            case ">=" -> x.compareTo(this.minX) >= 0;
            case "!=" -> true;
            default -> x.compareTo(this.minX) >= 0 && x.compareTo(this.maxX) < 0;
        };

        if(!xComparison) return false;

        yComparison = switch (operatorY) {
            case "<" -> y.compareTo(this.maxY) < 0;
            case "<=" -> y.compareTo(this.maxY) <= 0;
            case ">" -> y.compareTo(this.minY) > 0;
            case ">=" -> y.compareTo(this.minY) >= 0;
            case "!=" -> true;
            default -> y.compareTo(this.minY) >= 0 && y.compareTo(this.maxY) < 0;
        };

        if(!yComparison) return false;

        zComparison = switch (operatorZ) {
            case "<" -> z.compareTo(this.maxZ) < 0;
            case "<=" -> z.compareTo(this.maxZ) <= 0;
            case ">" -> z.compareTo(this.minZ) > 0;
            case ">=" -> z.compareTo(this.minZ) >= 0;
            case "!=" -> true;
            default -> z.compareTo(this.minZ) >= 0 && z.compareTo(this.maxZ) < 0;
        };

        return zComparison;
    }

    public Vector<Point> search(Comparable x, Comparable y, Comparable z, String operatorX, String operatorY, String operatorZ) {
        Vector<Point> searchResults = new Vector<>();

        // X > 5 | [0,6][7,10]
        if(!empty && belongs(x,y,z, operatorX, operatorY, operatorZ)) {
            if(leaf) {
                int i = 0;
                for(Object value : this.values.toArray()) {
                    Comparable[] realValue = (Comparable[]) value; // node value
                    i++;
                    boolean comparisonX = doComparison(realValue[0], x, operatorX);
                    if(!comparisonX) continue;
                    boolean comparisonY = doComparison(realValue[1], y, operatorY);
                    if(!comparisonY) continue;
                    boolean comparisonZ = doComparison(realValue[2], z, operatorZ);
                    if(!comparisonZ) continue;


                    searchResults.add(this.refs.get(i-1));
                }
            } else {
                for(Node childNode : childNodes) {
                    Vector<Point> searchResult = childNode.search(x,y,z, operatorX, operatorY, operatorZ);
                    searchResults.addAll(searchResult);
                }
            }
        }
        return searchResults;
    }

    private boolean doComparison(Comparable value1, Comparable value2, String operator) {
        boolean add = false;
        switch(operator) {
            case "=":
                add = value1.equals(value2);
                break;
            case "!=":
                add = !value1.equals(value2);
                break;
            case ">":
                add = value1.compareTo(value2) > 0;
                break;
            case ">=":
                add = value1.compareTo(value2) > 0 || value1.equals(value2);
                break;
            case "<":
                add = value1.compareTo(value2) < 0;
                break;
            case "<=":
                add = value1.compareTo(value2) < 0 || value1.equals(value2);
                break;
        }
        return add;
    }

    public void delete(Comparable x, Comparable y, Comparable z, Point ref) {
        if(!empty && belongs(x,y,z,"=","=","=")) {
            if(leaf) {
                int size = this.values.size();
                for(int i=0;i<size;i++) {
                    Comparable[] value = this.values.get(i);
                    if(x.compareTo(value[0]) == 0 && y.compareTo(value[1]) == 0 && z.compareTo(value[2]) == 0) {
                        Point currPoint = this.refs.get(i);
                        if(ref.equals(currPoint)) { // if reference is the head
                            this.refs.remove(i);
                            Comparable[] nodeValues = this.values.get(i);
                            this.values.remove(i);

                            if(currPoint.getPoint() != null) {
                                this.refs.add(currPoint.getPoint());
                                this.values.add(nodeValues);
                            }
                        } else { // if it isn't the head, but somewhere else in the linked list
                            while( (currPoint = currPoint.getPoint()) != null ) {
                                if(ref == currPoint) {
                                    ref.setPoint(ref.getPoint());
                                }
                            }
                        }
//                        this.values.remove(i);
//                        this.refs.remove(i);

                        if(this.values.size() == 0) {
                            this.selfDestruct();
                        }
                        return;
                    }
                }
            } else {
                int specificOctant = this.findChildOctantIndex(x,y,z); // Index of node where element to be deleted is found
                if(specificOctant == -1) return;
                childNodes[specificOctant].delete(x,y,z,ref);
                if(numChildren() == MAX_POINTS_PER_NODE) {
                    // get only child data, become me
                    Node n = this.getSingleChildData();
                    if(n==null) {
                        return;
                    }
                    this.values = n.values;
                    this.refs = n.refs;
                    this.destroyChildren();
                } else if(isEmpty()) {
                    this.selfDestruct();
                }
            }
        }
    }

    private void selfDestruct() {
        this.empty = true;
        this.leaf = true;
        this.refs = new Vector(MAX_POINTS_PER_NODE);
        this.childNodes = new Node[8];
        this.values = new Vector(MAX_POINTS_PER_NODE);
    }

    private void destroyChildren() {
        this.leaf = true;
        this.childNodes = new Node[8];
    }
    
    public void insert(Comparable x, Comparable y, Comparable z, Point ref) {
        if(this.belongs(x,y,z,"=","=","=")) {
            if(this.leaf) {
                if(this.hasSpace()) {
                    this.empty = false;
                    //System.out.println("Setting empty false");
                    boolean inserted = false;
                    for(Comparable[] value : this.values) {
                        if(value[0].equals(x) && value[1].equals(y) && value[2].equals(z)) { // handle duplicates
                            int idx = this.values.indexOf(value);
                            Point pt = this.refs.get(idx);
                            pt.setPoint(ref);
                            inserted = true;
                            break;
                        }
                    }
                    if(!inserted) {
                        this.values.add(new Comparable[] {x,y,z});
                        this.refs.add(ref);
                        inserted = true;
                    }
                } else {
                    //System.out.println("Splitting");
                    this.split(x,y,z,ref);
                }
            } else {
                for (Node childNode: childNodes) {
                    childNode.insert(x,y,z, ref);
                }
            }
        }
    }

    private boolean hasSpace() {
        if(this.values.size() < MAX_POINTS_PER_NODE) {
            return true;
        }
        return false;
    }

    private boolean isEmpty() {
        if(leaf) {
            return empty;
        } else {
            for(Node childNode : childNodes) {
                if(!childNode.isEmpty()) {
                    return false;
                }
            }
            return true;
        }
    }

    private int numChildren() {
        int r = 0;
        for(Node childNode : childNodes) {
            if(childNode.leaf) {
                r += childNode.values.size();
            } else {
                r += childNode.numChildren();
            }
        }
        return r;
    }

    private Node getSingleChildData() {
        try {
            Node n = new Node(0,0,0,0,0,0);

            for(Node childNode : childNodes) {
                for(Comparable[] value : childNode.values) {
                    n.values.add(value);
                }
                for(Point ref : childNode.refs) {
                    n.refs.add(ref);
                }
            }
            return n;
        } catch(Exception e) {
            System.out.println("Unexpected error while trying to create a node in Node.java: " + e.getMessage());
            return null;
        }

    }
    private int findChildOctantIndex(Comparable x, Comparable y, Comparable z) {
        int i = 0;
        for(Node childNode : childNodes) {
            if(childNode.belongs(x,y,z,"=","=","=")) {
                return i;
            }
            i++;
        }
        return -1;
    }


    private void split(Comparable newX, Comparable newY, Comparable newZ, Point newRef) {
        // integer, double, date, string
        this.leaf = false;
        Comparable medianX = getMedian(minX, maxX);
        Comparable medianY = getMedian(minY, maxY);
        Comparable medianZ = getMedian(minZ, maxZ);

        try {
            childNodes[0] = new Node(minX, medianX, minY, medianY, minZ, medianZ); // 000
            childNodes[1] = new Node(minX, medianX, minY, medianY, medianZ, maxZ); // 001
            childNodes[2] = new Node(minX, medianX, medianY, maxY, minZ, medianZ); // 010
            childNodes[3] = new Node(minX, medianX, medianY, maxY, medianZ, maxZ); // 011
            childNodes[4] = new Node(medianX, maxX, minY, medianY, minZ, medianZ); // 100
            childNodes[5] = new Node(medianX, maxX, minY, medianY, medianZ, maxZ); // 101
            childNodes[6] = new Node(medianX, maxX, medianY, maxY, minZ, medianZ); // 110
            childNodes[7] = new Node(medianX, maxX, medianY, maxY, medianZ, maxZ); // 111
        } catch(Exception e) {
            System.out.println("Unexpected error occured while creating node in Node.java: " + e.getMessage());
            return;
        }
        int i = 0;
        for(Object value : this.values.toArray()) {
            Comparable[] realValue = (Comparable[]) value;
            childNodes[this.findChildOctantIndex(realValue[0],realValue[1],realValue[2])].insert(realValue[0],realValue[1],realValue[2], this.refs.get(i));
            i++;
        }
        childNodes[this.findChildOctantIndex(newX,newY,newZ)].insert(newX,newY,newZ,newRef);
    }

    private Comparable getMedian(Comparable min, Comparable max) {
        if(min instanceof String) {
            return computeMedianString((String) min, (String) max);
        } else if(min instanceof Double) {
            return computeMedianDouble((Double) min, (Double) max);
        } else if(min instanceof Integer) {
            return computeMedianInteger((Integer) min, (Integer) max);
        } else {
            return computeMedianDate((Date) min, (Date) max);
        }
    }

    private Integer computeMedianInteger(Integer x1, Integer x2) {
        return (x1 + x2) / 2;
    }

    private Double computeMedianDouble(Double x1, Double x2) {
        return (x1 + x2) / 2;
    }

    private String computeMedianString(String x1, String x2) {
        return Utilities.average(x1, x2);
    }

    private Date computeMedianDate(Date x1, Date x2) {
        return new Date((x1.getTime() + x2.getTime()) / 2);
    }
}
