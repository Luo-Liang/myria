package edu.washington.escience.myria.operator.agg;

import com.google.common.collect.ImmutableMap;
import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.BatchTupleSource;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.Sample;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.tools.MyriaWorkerConfigurationModule;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Created by liangluo on 11/3/2016.
 */
public class SketchAdviser {
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(Operator.class);
    private static final Schema LEFT_SCHEMA =
            Schema.ofFields(
                    "WorkerID",
                    Type.INT_TYPE,
                    "PartitionSize",
                    Type.INT_TYPE,
                    "SampleSize",
                    Type.INT_TYPE,
                    "SampleType",
                    Type.STRING_TYPE);
    Operator underlyingRelation;
    private static final long RANDOM_SEED = 42;
    private static final int SAMPLE_SIZE = SketchBuffer.DEFAULT_COLUMN * SketchBuffer.DEFAULT_ROWS;

    public SketchAdviser(final Operator underlyingRelation) {
        this.underlyingRelation = underlyingRelation;
    }

    //assuming independence of values for each group fields.
    public boolean shouldSketch(int cellCount, int[] gfields, Map<String, Object> environment) {
        int relationLength = 0;
        double multiplicationRatio = 1;
        try {
            underlyingRelation.close();
            underlyingRelation.open(environment);
            while (!underlyingRelation.eos()) {
                TupleBatch batch = underlyingRelation.nextReady();
                relationLength += batch.numTuples();
            }
            if (relationLength < cellCount) {
                return false;
            }
            if (relationLength > SAMPLE_SIZE) {
                multiplicationRatio = relationLength / SAMPLE_SIZE;
            }
        } catch (Exception e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Problem resetting iterator for underlyingRelation for advice. {}", e);
            }
            return true;
        }
        TupleBatchBuffer leftInput = new TupleBatchBuffer(LEFT_SCHEMA);
        //TODO: put a real worker id here instead of 0.
        leftInput.putInt(1, relationLength);
        leftInput.putInt(2, SAMPLE_SIZE);
        leftInput.putString(3, "WithoutReplacement");
        Sample sampOp = new Sample(new BatchTupleSource(leftInput), underlyingRelation, RANDOM_SEED);
        HashSet[] counters = new HashSet[gfields.length];
        try {
            sampOp.open(environment);
            while (!sampOp.eos()) {
                TupleBatch tb = sampOp.nextReady();
                //how many distinct items are there?
                for (int i = 0; i < tb.numTuples(); ++i) {
                    for (int col = 0; col < gfields.length; col++) {
                        int column = gfields[col];
                        counters[col].add(tb.getObject(column, i));
                    }
                }
            }
            //now multiply them as they are independent.
            double estimatedSampleLength = multiplicationRatio;
            for (int i = 0; i < counters.length; i++) {
                estimatedSampleLength *= counters[i].size();
            }
            if (estimatedSampleLength > cellCount) {
                return true;
            } else {
                return false;
            }
        } catch (DbException e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Problem opening iterator for sample of sampOp {}.", e);
            }
            return true;
        }

    }

}
