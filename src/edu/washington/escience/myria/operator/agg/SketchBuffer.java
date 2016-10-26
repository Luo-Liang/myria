package edu.washington.escience.myria.operator.agg;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.Tuple;
import edu.washington.escience.myria.util.HashUtils;

/**
 * Created by Liang Luo Local on 10/23/2016.
 */
public class SketchBuffer {
    public final static int DEFAULT_ROWS = 10;
    public final static int DEFAULT_COLUMN = 500;
    private Object[][] sketchArrays;
    private int hashFunctionsCount;
    private int rowSize;
    private Aggregator aggregator;

    public SketchBuffer(int hashRows, int rowSize, Aggregator agg) {
        if (hashRows > HashUtils.NUM_OF_HASHFUNCTIONS)
            throw new IllegalArgumentException("Has only " + HashUtils.NUM_OF_HASHFUNCTIONS + " hash functions defined.");
        sketchArrays = new IntegerAggregator.IntAggState[hashRows][rowSize];
        hashFunctionsCount = hashRows;
        this.rowSize = rowSize;
        aggregator = agg;
    }

    public Object[] getStates(final Object key, final Type type) {
        int[] familyHashValues = HashUtils.hashValueFamily(key, type, hashFunctionsCount);
        Object[] results = new Object[hashFunctionsCount];
        for (int r = 0; r < hashFunctionsCount; r++) {
            int column = familyHashValues[r] % rowSize;
            if (sketchArrays[r][column] == null) {
                sketchArrays[r][column] = aggregator.getInitialState();
            }
            results[r] = sketchArrays[r][column];
        }
        return results;
    }
}
