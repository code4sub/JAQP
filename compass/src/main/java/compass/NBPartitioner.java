package compass;

import java.util.*;

public class NBPartitioner  extends VariancePartitioner{
    //the Nested Binary search partitioner

    public List<HyperRectangle> partitionTo(int k, String predAttribute){
        if(predAttribute.compareTo(this.predAttr) != 0){
            System.out.println("Mismatch predicate attribute: " + predAttribute + " vs " + this.predAttr);
            System.exit(1);
        }

        if(!initialized){
            System.out.println("NBPartitioner not initialized.");
            System.exit(1);
        }
        maxVarCache = new HashMap<String, Double>();
        sample = sample.sortAscendingOn(predAttribute);
        System.out.println("NBPartitioner k="+k +", samplesize:"+sample.rowCount()+", "+minSamplePerBucket+" per bucket");
        if(minSamplePerBucket*k > sample.rowCount()){
            System.out.println("Not enough sample for k = " + k + ", need at least " + (int)(minSamplePerBucket*k*1.2) + " samples to work with");
            System.exit(1);
        }

        List<HyperRectangle> hr = null;

        double minVar = lowerBoundVar;
        double maxVar = upperBoundVar;
        System.out.println(minVar + ", " + maxVar);
        double mid = 0.0;
        foundBest = false;
        // int count = 0;
        while(minVar <= maxVar){
            // count += 1;
            if(maxVarianceFromSearch < maxVar && maxVarianceFromSearch > minVar){
                maxVar = maxVarianceFromSearch;
            }
            maxVarianceFromSearch = Double.MAX_VALUE; //get another update
            mid = minVar + (maxVar - minVar)/2;

            List<HyperRectangle> newHr = findPartitionForVar(mid, k);
            if(newHr != null){
                if(newHr.size() < k){
                    if(hr == null || hr.size() != k)
                        hr = newHr; //better than nothing
                    maxVar = mid-0.1;
                }else if(newHr.size() > k){
                    if(hr == null || hr.size() != k || foundBest == false){
                        newHr.remove(newHr.size()-1);
                        hr = newHr;
                    }
                    minVar = mid+0.1;
                }else{
                    hr = newHr;
                    foundBest = true;
                    // break;
                    maxVar = mid-1;
                }
            }else{
                minVar = mid+1;
            }
        }
        return hr;
    }

    List<HyperRectangle> findPartitionForVar(double var, int k){
        List<HyperRectangle> hr = new ArrayList<HyperRectangle>();
        int ith = 0;
        double left = -Double.MAX_VALUE;
        double right = -1;
        for(int i = 0 ; i < k; i++){
            if( i > 0 ) left = right;
            int end = findNextBucket(ith, var);
            int start = ith;
            if(end < 0) return null;
            right = sample.row(end).getDouble(predAttr);

            if( i == k-1){
                if(end < sample.rowCount()-1){
                    end = sample.rowCount() - 1;
                    HyperRectangle h = new HyperRectangle();
                    h.addDimension(predAttr, left, Double.MAX_VALUE);
                    hr.add(h);
                    //size of k+1, to indicate given var is small,
                    //we couldn't use all samples
                    hr.add(new HyperRectangle());
                    maxVarCache = new HashMap<String, Double>();
                    break;
                }
                right = Double.MAX_VALUE;
            }

            ith = end + 1;
            HyperRectangle h = new HyperRectangle();
            h.addDimension(predAttr, left, right);

            // System.out.println("#" + (i+1) + "/"  + k + ":" +start + " => " + end + " size: " + (end-start+1) + ", [" + left + ", " + right + ")");

            hr.add(h);
            if(ith >= sample.rowCount()-1){
                if(i == k-1){
                //     System.out.println("Found a valid partition for var: " + var);
                    // System.out.println("#" + (i+1) + "/"  + k + ":" +start + " => " + end + " size: " + (end-start+1) + ", [" + left + ", " + right + ")");
                }
                break;
            }
        }
        return hr;
    }

    int findNextBucket(int start, double var){
        //return the bucket and the right most sample (inclusive)
        int ith = start;
        int jth = sample.rowCount()-1;
        //binary search to find the next interval
        int end = -1;
        int mid = -1;

        double intVar = estimator.getIntervalVariance(ith, jth);
        if(intVar <= var){
            if(maxVarianceFromSearch == Double.MAX_VALUE
                || maxVarianceFromSearch > intVar)
                if(intVar != lowerBoundVar) maxVarianceFromSearch = intVar;
            return jth;
        }
        double maxVarOfCurrentSearch = 0.0;

        //correct the variance estimator so it is always in bound and monotonic
        double leftBoundVar = 0.0, rightBoundVar = Double.MAX_VALUE;

        while(ith <= jth){
            mid = (ith + jth)/2;
            intVar = estimator.getIntervalVariance(start, mid);
            if(intVar < leftBoundVar){
                intVar = leftBoundVar + 1.0;

            }
            if(intVar > rightBoundVar){
                intVar = rightBoundVar - 1.0;
            }
            if(intVar == var){
                end = mid;
                if(maxVarOfCurrentSearch < intVar)
                    maxVarOfCurrentSearch = intVar;
                break;
            }else if(intVar < var){
                //smaller variance, grow the interval
                //due to monotonicity of maximum variance, larger interval leads to larger max variance
                ith = mid + 1;
                end = mid;
                //moving the left bound, increase the lefBoundVar
                leftBoundVar = intVar;
                if(maxVarOfCurrentSearch < intVar)
                    maxVarOfCurrentSearch = intVar;
            }else{
                //moving the right bound, decrease the rightBoundVar
                rightBoundVar = intVar;
                jth = mid - 1;
            }
        }
        if(maxVarianceFromSearch < maxVarOfCurrentSearch)
            maxVarianceFromSearch = maxVarOfCurrentSearch;
        return end;
    }

}