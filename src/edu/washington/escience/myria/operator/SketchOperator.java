package edu.washington.escience.myria.operator;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.operator.agg.*;
import edu.washington.escience.myria.storage.TupleBatch;

import javax.annotation.Nullable;

/**
 * Created by liangluo on 11/22/2016.
 */
public class SketchOperator extends UnaryOperator
{
    //Due to a request change, we need to use this new sketch buffer for ease of use.
    static class RawSketchBuffer
    {
        Long[][] Counters;
        public RawSketchBuffer(int hashRows, int rowSize)
        {
            Counters = new Long[hashRows][rowSize];
        }
        
    }
    public static final String HashFunction = "HASH_FUN_IDX";
    public static final String Bucket = "BUCKET";
    public static final String Count = "COUNT";
    int[] groupColumns;
    public SketchOperator(@Nullable final Operator child, final int[] gfields, AggregationSketchOption option)
    {
        super(child);
        this.groupColumns = gfields;
        //regardless of request, do a sketch
        //agg is simply MINSKETCH
        Aggregator agg = new IntegerAggregator(Count, new PrimitiveAggregator.AggregationOp[]{PrimitiveAggregator.AggregationOp.COUNT},-1);

        SketchBuffer sketchBuffer = new SketchBuffer(SketchBuffer.DEFAULT_ROWS, SketchBuffer.DEFAULT_COLUMN, null);
    }
    public SketchOperator(@Nullable final Operator child, final int[] gfields)
    {
       this(child,gfields,AggregationSketchOption.UseSketchMin);
    }
    @Override
    protected TupleBatch fetchNextReady() throws Exception
    {
        return null;
    }

    @Override
    protected Schema generateSchema()
    {
        return null;
    }
}
