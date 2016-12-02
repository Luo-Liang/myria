package edu.washington.escience.myria.operator.agg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.UnaryOperator;
import edu.washington.escience.myria.operator.agg.*;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.HashUtils;

import javax.annotation.Nullable;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Created by liangluo on 11/22/2016.
 */
public class SketchOperator extends UnaryOperator
{
    //Due to a request change, we need to use this new sketch buffer for ease of use.
    static class RawSketchBuffer
    {
        int[][] Counters;
        public RawSketchBuffer(int hashRows, int rowSize)
        {
            Counters = new int[hashRows][rowSize];
        }
        
    }
    public static final String HashFunction = "HASH_FUN_IDX";
    public static final String Bucket = "HASH_VAL";
    public static final String Count = "COUNT";
    int[] groupColumns;
    private transient TupleBatchBuffer resultBuffer;
    private transient RawSketchBuffer sketchBuffer;

    public SketchOperator(@Nullable final Operator child)
    {
        super(child);
        System.out.println("BUILDING SKETCH OPERATOR");
    }
    @Override
    protected TupleBatch fetchNextReady() throws Exception
    {
        TupleBatch tb = null;
        final Operator child = getChild();

        if (resultBuffer.numTuples() > 0) {
            return resultBuffer.popAny();
        }

        if (child.eos()) {
            return null;
        }

        while ((tb = child.nextReady()) != null) {
            processTupleBatch(tb);
        }

        if (child.eos()) {
            generateResult(resultBuffer);
        }
        return resultBuffer.popAny();
    }

    private void generateResult(TupleBatchBuffer resultBuffer) {
        for(int row = 0; row < sketchBuffer.Counters.length; row++)
        {
            for(int col =0; col < sketchBuffer.Counters[row].length; col++)
            {
                resultBuffer.putInt(0,row);
                resultBuffer.putInt(1,col);
                resultBuffer.putInt(2, sketchBuffer.Counters[row][col]);
                System.out.print(row + "," + col + "," + sketchBuffer.Counters[row][col]);
            }
        }
        System.out.println();
    }

    private void processTupleBatch(TupleBatch tb) {
        for (int i = 0; i < tb.numTuples(); ++i) {
            int[] rowHashes = HashUtils.hashSubRowFamily(tb, groupColumns, i,SketchBuffer.DEFAULT_ROWS);
            for(int hid = 0; hid < SketchBuffer.DEFAULT_ROWS; hid++)
            {
                int column =  ((rowHashes[hid] % SketchBuffer.DEFAULT_COLUMN) + SketchBuffer.DEFAULT_COLUMN) % SketchBuffer.DEFAULT_COLUMN;
                sketchBuffer.Counters[hid][column]++;
            }
        }
    }
    @Override
    protected final void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
        resultBuffer = new TupleBatchBuffer(getSchema());
        this.groupColumns = IntStream.range(0,getChild().getSchema().numColumns()).toArray();
        sketchBuffer = new RawSketchBuffer(SketchBuffer.DEFAULT_ROWS, SketchBuffer.DEFAULT_COLUMN);
        //regardless of request, do a sketch
    }

    @Override
    protected Schema generateSchema()
    {
        //first column is hashid, 2nd is column, 3rd is cnt.
        //int, int, int
        List<Type> columnTypes = new ArrayList<>();
        columnTypes.add(Type.INT_TYPE);
        columnTypes.add(Type.INT_TYPE);
        columnTypes.add(Type.INT_TYPE);

        List<String> columnNames = new ArrayList<>();
        columnNames.add(HashFunction);
        columnNames.add(Bucket);
        columnNames.add(Count);
        Schema schema = new Schema(columnTypes,columnNames);
        return schema;
    }
}
