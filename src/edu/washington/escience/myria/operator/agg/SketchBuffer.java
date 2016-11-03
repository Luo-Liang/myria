package edu.washington.escience.myria.operator.agg;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.ReadableTable;
import edu.washington.escience.myria.storage.Tuple;
import edu.washington.escience.myria.util.HashUtils;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.util.Arrays;

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
            throw new IllegalArgumentException(
                    "Has only " + HashUtils.NUM_OF_HASHFUNCTIONS + " hash functions defined.");
        sketchArrays = new IntegerAggregator.IntAggState[hashRows][rowSize];
        hashFunctionsCount = hashRows;
        this.rowSize = rowSize;
        aggregator = agg;
    }

    private Object[] retrieve(int[] columnIndices, boolean countSketch)
    {
      Object[] results = new Object[hashFunctionsCount];
      for (int r = 0; r < hashFunctionsCount; r++) {
        int column = columnIndices[r] % rowSize;
        if (countSketch) {
          if ((column & 1) == 0) column--;
          else column++;
        }
        if (column < 0) {
          column += rowSize;
        }
        if (sketchArrays[r][column] == null) {
          sketchArrays[r][column] = aggregator.getInitialState();
        }
        results[r] = sketchArrays[r][column];
      }
      return results;
    }

    private Object[] getStatesByKey(final Object key, final Type type, boolean countSketch) {
        int[] familyHashValues = HashUtils.hashValueFamily(key, type, hashFunctionsCount);
        return retrieve(familyHashValues,countSketch);
    }

    private Object[] getStatesByKeys(final ReadableTable table,int row, final int[] columns, boolean countSketch)
    {
        int[] familyHashValues = HashUtils.hashSubRowFamily(table,columns,row,hashFunctionsCount);
        return retrieve(familyHashValues,countSketch);
    }

    public Object[] getCountMinStatesByRow(final ReadableTable table,int row, final int[] columns)
    {
        return getStatesByKeys(table,row,columns,false);
    }

    public Object[] getCountMinStates(final Object key, final Type type) {
        return getStatesByKey(key, type, false);
    }

    public Object[] getCountStates(final Object key, final Type type) {
        Object[] operands1 = getStatesByKey(key, type, false);
        Object[] operands2 = getStatesByKey(key, type, true);
        //now get another set of estimates and perform a diff.
        Object[] results = new Object[2];
        results[0] = operands1;
        results[1] = operands2;
        return results;
    }
}
