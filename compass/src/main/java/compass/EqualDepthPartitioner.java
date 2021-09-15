package compass;

import tech.tablesaw.api.*;
import java.util.*;

public class EqualDepthPartitioner extends SamplePartitioner{
    public List<HyperRectangle> partitionTo(int k, String predAttribute){
        // Todo: iterate the sample then split then into equal depth partition.
        List<HyperRectangle> hr = new ArrayList<HyperRectangle>();
        // sample = sample.retainColumns(predAttribute).dropDuplicateRows(); should not remove duplicate in case of skewed duplicates.
        Table sorted = sample.sortAscendingOn(predAttribute);

        int bucketSize = sample.rowCount()/k;
        // we need to make sure we cover the entire domain of [-inf, inf], assuming it is discrete.
        double left = -Double.MAX_VALUE;
        double right = Double.MAX_VALUE;
        for(int i = 0; i < k; i++){
            if(i > 0)
                left = right;
            if(i == k-1)
                right = Double.MAX_VALUE;
            else
                right = sorted.row((i+1)*bucketSize - 1).getDouble(predAttribute);

            HyperRectangle h = new HyperRectangle();
            h.addDimension(predAttribute, left, right);
            hr.add(h);
        }
        return hr;
    }
}
