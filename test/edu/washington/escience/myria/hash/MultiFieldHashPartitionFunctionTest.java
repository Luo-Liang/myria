package edu.washington.escience.myria.hash;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.TupleSource;
import edu.washington.escience.myria.operator.network.partition.HashPartitionFunction;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

public class MultiFieldHashPartitionFunctionTest {

  private static final int NUM_PARTITIONS = 3;
  private Random rand; // for randomizing numbers

  @Before
  public void setUp() throws Exception {
    rand = new Random();
  }

  @Test
  public void testMultiFieldPartitionFunction() {
    HashPartitionFunction multiFieldPartitionFunction = new HashPartitionFunction(new int[] { 0, 1 });
    multiFieldPartitionFunction.setNumDestinations(NUM_PARTITIONS);
    int numGroups = rand.nextInt(10) + 1;
    int tuplesPerGroup = rand.nextInt(10) + 1;
    TupleSource source = generateTupleBatchSource(numGroups, tuplesPerGroup);
    try {
      source.open(null);
      TupleBatch tb = source.nextReady();
      assertNotNull(tb);
      int expected = -1;
      for (int i = 0; i < tb.numTuples(); ++i) {
        int p = multiFieldPartitionFunction.distribute(tb, i)[0];
        if (i % tuplesPerGroup == 0) {
          expected = p;
        } else {
          assertEquals(expected, p);
        }
      }
    } catch (DbException e) {
      throw new RuntimeException(e);
    }
  }

  /*
   * Generates a tuple batch source with the following schema: a (int), b (int), c (int)
   */
  private TupleSource generateTupleBatchSource(final int numGroups, final int tuplesPerGroup) {
    final Schema schema = new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE), ImmutableList.of(
        "a", "b", "c"));
    TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < numGroups; i++) {
      for (int j = 0; j < tuplesPerGroup; j++) {
        tbb.putInt(0, i);
        tbb.putInt(1, i + 1);
        tbb.putInt(2, rand.nextInt());
      }
    }
    return new TupleSource(tbb);
  }

}
