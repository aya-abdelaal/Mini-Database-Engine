package components;

import java.io.Serializable;

public class Point implements Serializable {
    private int page;
    private Object id;
    private Point point = null;
    public static int NUM_POINTS = 0;

    public Point(int page, Object id) {
        this.page = page;
        this.id = id;
        NUM_POINTS++;
    }

    public void setPage(int page) {
        this.page = page;
    }

    public Object getId() {
        return this.id;
    }

    public int getPage() {
        return this.page;
    }

    public void setPoint(Point pt) {
        if(this.point == null) {
            this.point = pt;
        } else {
            this.point.setPoint(pt);
        }
    }

    @Override
    public boolean equals(Object o) {
        if(! (o instanceof Point)) return false;
        Point pt = (Point) o;
        if( pt.page == this.page && pt.id.equals(this.id) ) {
            return true;
        }
        return false;
    }

        public Point getPoint() {
        return this.point;
    }

}
