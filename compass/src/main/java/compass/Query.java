package compass;

import java.util.*;


class QueryResult{
    double cnt;
    double sum;
    double min;
    double max;

    public QueryResult(){};
    public QueryResult(double c, double s, double mi, double ma){
        cnt = c; sum = s; min = mi; max = ma;
    }

    // public synchronized void merge(double c, double s, double mi, double ma){
    //     cnt += c; sum += s; min = Math.min(mi, min); Math.max(max, ma);
    // }

    public void merge(double[] stats){
        cnt += stats[0]; sum += stats[1];
        min = Math.min(stats[2], min); Math.max(max, stats[3]);
    }

    public void minus(double[] stats){
        cnt -= stats[0]; sum -= stats[1];
        //there's no accurate way to handle min/max, just leave it as is.
    }

    public double[] asArray(){
        double[] ret = {cnt, sum, sum/cnt, min, max};
        return ret;
    }

    public double[] asArrayWithTime(long time){
        double[] ret = {cnt, sum, sum/cnt, min, max, time};
        return ret;
    }

    public String toString(){
        return String.format("CNT: %.2f; SUM: %.2f; AVG: %.2f",
        cnt, sum, sum/cnt);
    }
}

public class Query {

    HyperRectangle predicates;
    String aggrAttribute;
    String hash;
    String qs;
    int nDimensions;
    public HyperRectangle getPredicates(){return predicates;}
    public Query(String s){
        hash = org.apache.commons.codec.digest.DigestUtils.sha256Hex(s).substring(0, 10);
        qs = s.trim();
        s = s.replace("[","").replace("]", "");
        predicates = new HyperRectangle();
        String[] tokens = s.split(";");
        aggrAttribute = tokens[0].trim();
        for(String t : Arrays.copyOfRange(tokens, 1, tokens.length)){
            String[] ts = t.split(":");
            String attr = ts[0].trim();
            String[] nums = ts[1].split(",");
            predicates.addDimension(attr, Double.parseDouble(nums[0]), Double.parseDouble(nums[1]));
            nDimensions += 1;
            // Map.Entry<Double, Double> e = new AbstractMap.SimpleEntry<Double, Double>(Double.parseDouble(nums[0]), Double.parseDouble(nums[1]));
            // predicates.put(attr, e);
        }
    }

    public Query(){}
    public Query getSubquery(HyperRectangle pred){
        Query sub = new Query();
        sub.predicates = pred;
        sub.aggrAttribute = aggrAttribute;
        sub.hash = org.apache.commons.codec.digest.DigestUtils.sha256Hex(pred.toString()+aggrAttribute).substring(0,10);
        return sub;
    }
    public String getQueryString(){
        return qs;
    }
    int getNDimensions(){ return nDimensions; }
    String getHash(){
        return hash;
    }
    public String toString() {
        return hash + " : aggr(" + aggrAttribute +"), " + predicates.toString();
    }
}

interface QueryAdapter{
    public abstract Query stringToQuery(String s);
}

