package compass;
import java.text.DecimalFormat;
import java.util.*;
public class HyperRectangle {
    //! the range predicate is start inclusive and end exclusive, also easier to work with tablesaw filter
    Map<String, Map.Entry<Double, Double>> rectangle;

    public HyperRectangle(){
        rectangle = new HashMap<String, Map.Entry<Double,Double>>();
    }

    public Map<String, Map.Entry<Double, Double> > getRectangles(){
        return rectangle;
    }

    public HyperRectangle(HyperRectangle hr){
        rectangle = new HashMap<String,Map.Entry<Double,Double>>(hr.getRectangles());
    }

    public void addDimension(String name, Double l, Double r){
        Map.Entry<Double, Double> e = new AbstractMap.SimpleEntry<Double, Double>(l, r);
        rectangle.put(name, e);
    }

    public void removeDimension(String name){
        rectangle.remove(name);
    }

    public boolean contains(String name){
        return rectangle.containsKey(name);
    }

    public Map.Entry<Double, Double> getDimension(String name){
        return rectangle.get(name);
    }

    public Set<String> getDimensionNames(){
        return rectangle.keySet();
    }

    public HyperRectangle intersection(HyperRectangle other){
        HyperRectangle hr = new HyperRectangle();
        for (Map.Entry<String, Map.Entry<Double, Double> > pair : rectangle.entrySet()) {
            String name = pair.getKey();
            if(dimensionIntersects(rectangle.get(name), other.getDimension(name)) == false){
                // System.out.println(toString() + " intersect " + other + "\n===> Empty intersection because HRs are disjoint.");
                return null; //new HyperRectangle();
            }
            double l = Math.max(rectangle.get(name).getKey(), other.getDimension(name).getKey());
            double r = Math.min(rectangle.get(name).getValue(), other.getDimension(name).getValue());
            //two hyperrect are disjoint iff any common attr is disjoint
            hr.addDimension(name, l, r);
        }
        return hr;
    }

    public HyperRectangle merge(HyperRectangle other){
        //* i.e. the union of two hrs over shared attributes
        HyperRectangle hr = new HyperRectangle();
        for (Map.Entry<String, Map.Entry<Double, Double> > pair : rectangle.entrySet()) {
            String name = pair.getKey();
            double l = Math.min(rectangle.get(name).getKey(), other.getDimension(name).getKey());
            double r = Math.max(rectangle.get(name).getValue(), other.getDimension(name).getValue());
            //two hyperrect are disjoint iff any common attr is disjoint
            hr.addDimension(name, l, r);
        }
        return hr;
    }

    boolean dimensionIntersects(Map.Entry<Double, Double> p1, Map.Entry<Double, Double> p2){
        double l1, r1, l2, r2;
        l1 = p1.getKey();
        r1 = p1.getValue();
        l2 = p2.getKey();
        r2 = p2.getValue();
        //left closed, right open means two intervals are disjoint even if min(r1,r2) == max(l1, l2)
        if(Math.min(r1, r2) <= Math.max(l1, l2))
            return false;
        return true;
    }

    boolean dimensionCovers(Map.Entry<Double, Double> p1, Map.Entry<Double, Double> p2){
        double l1, r1, l2, r2;
        l1 = p1.getKey();
        r1 = p1.getValue();
        l2 = p2.getKey();
        r2 = p2.getValue();
        if(l1<=l2 && r1>=r2) return true;
        return false;
    }

    boolean covers(HyperRectangle other){
        Set<String> otherDimensionNames = other.getDimensionNames();
        Set<String> dimensionNames = rectangle.keySet();
        if(dimensionNames.equals(otherDimensionNames)){
            for (Map.Entry<String, Map.Entry<Double, Double> > pair : rectangle.entrySet()) {
                String name = pair.getKey();
                if(dimensionCovers(rectangle.get(name), other.getDimension(name)) == false) {
                    return false;
                }
            }
            return true;
        }else if(otherDimensionNames.containsAll(dimensionNames)){
            return true;
        }
        //when we get here, the two dimension sets are different, could be orthogonal or we have more expliicit dimension than other. therefore fail to cover.
        return false;
    }

    boolean intersects(HyperRectangle other){
        for (Map.Entry<String, Map.Entry<Double, Double> > pair : rectangle.entrySet()) {
            String name = pair.getKey();
            //two hyperrect are disjoint iff any common attr is disjoint
            if(other.contains(name) && dimensionIntersects(rectangle.get(name), other.getDimension(name)) == false) {
                return false;
            }
        }
        return true;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        DecimalFormat df = new DecimalFormat("#.##");

        for (Map.Entry<String, Map.Entry<Double, Double> > pair : rectangle.entrySet()) {
            String name = pair.getKey();
            String lv = df.format(rectangle.get(name).getKey());
            if(rectangle.get(name).getKey()==-Double.MAX_VALUE) lv = "MIN";

            String rv = df.format(rectangle.get(name).getValue());
            if(rectangle.get(name).getValue()==Double.MAX_VALUE) rv = "MAX";
            sb.append(name+": ["+lv+", "+rv+"), ");
        }
        return sb.toString();
    }
}
