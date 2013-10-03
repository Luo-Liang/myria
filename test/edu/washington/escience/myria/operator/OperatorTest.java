package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Test;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multiset;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.util.TestUtils;

public class OperatorTest {
  /**
   * 
   * @param numTuples
   * @param sorted Generate sorted tuples, sorted by id
   * @return
   */
  public TupleBatchBuffer generateRandomTuples(final int numTuples, boolean sorted) {
    final String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, numTuples, 20);
    final long[] ids = TestUtils.randomLong(0, 2000, names.length);

    if (sorted) {
      Arrays.sort(ids);
    }

    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.put(0, ids[i]);
      tbb.put(1, names[i]);
    }
    return tbb;
  }

  @Test
  public void testUnionAllCount() throws DbException {
    TupleSource[] children = new TupleSource[3];
    children[0] = new TupleSource(generateRandomTuples(123, false));
    children[1] = new TupleSource(generateRandomTuples(42, false));
    children[2] = new TupleSource(generateRandomTuples(99, false));
    UnionAll union = new UnionAll(children);
    union.open(null);
    TupleBatch tb = null;
    int count = 0;
    while (!union.eos()) {
      tb = union.nextReady();
      if (tb != null) {
        count += tb.numTuples();
      }
    }
    union.close();
    assertEquals(123 + 42 + 99, count);
  }

  @Test
  public void testUnionAllCorrectTuples() throws DbException {
    TupleBatchBuffer[] randomTuples = new TupleBatchBuffer[2];
    randomTuples[0] = generateRandomTuples(123, false);
    randomTuples[1] = generateRandomTuples(42, false);

    TupleSource[] children = new TupleSource[2];
    children[0] = new TupleSource(randomTuples[0]);
    children[1] = new TupleSource(randomTuples[1]);

    UnionAll union = new UnionAll(children);
    union.open(null);
    TupleBatch tb;

    Multiset<Long> actualCounts = HashMultiset.create();
    while (!union.eos()) {
      tb = union.nextReady();
      if (tb != null) {
        for (int i = 0; i < tb.numTuples(); i++) {
          long index = tb.getLong(0, i);
          actualCounts.add(index);
        }
      }
    }
    union.close();

    Multiset<Long> expectedCounts = HashMultiset.create();
    for (TupleBatchBuffer randomTuple : randomTuples) {
      for (TupleBatch tuples : randomTuple.getAll()) {
        for (int j = 0; j < tuples.numTuples(); j++) {
          Long index = tuples.getLong(0, j);
          expectedCounts.add(index);
        }
      }
    }

    for (Multiset.Entry<Long> expectedEntry : expectedCounts.entrySet()) {
      assertEquals(expectedEntry.getCount(), actualCounts.count(expectedEntry.getElement()));
    }
  }

  @Test
  public void testMergeCount() throws DbException {
    TupleSource[] children = new TupleSource[3];
    children[0] = new TupleSource(generateRandomTuples(12300, true));
    children[1] = new TupleSource(generateRandomTuples(4200, true));
    children[2] = new TupleSource(generateRandomTuples(9900, true));
    NAryOperator merge = new Merge(children, new int[] { 0 }, new boolean[] { true });
    merge.open(null);
    TupleBatch tb = null;
    int count = 0;
    while (!merge.eos()) {
      tb = merge.nextReady();
      if (tb != null) {
        count += tb.numTuples();
      }
    }
    merge.close();
    assertEquals(12300 + 4200 + 9900, count);
  }

  @Test
  public void testMergeTuplesSorted() throws DbException {
    TupleBatchBuffer[] randomTuples = new TupleBatchBuffer[3];
    randomTuples[0] = generateRandomTuples(12300, true);
    randomTuples[1] = generateRandomTuples(4200, true);
    randomTuples[2] = generateRandomTuples(9900, true);

    TupleSource[] children = new TupleSource[3];
    children[0] = new TupleSource(randomTuples[0]);
    children[1] = new TupleSource(randomTuples[1]);
    children[2] = new TupleSource(randomTuples[2]);

    NAryOperator merge = new Merge(children, new int[] { 0 }, new boolean[] { true });
    merge.open(null);
    TupleBatch tb;
    ArrayList<Long> idCollector = new ArrayList<Long>();
    while (!merge.eos()) {
      tb = merge.nextReady();
      if (tb != null) {
        for (int i = 0; i < tb.numTuples(); i++) {
          idCollector.add(tb.getLong(0, i));
        }
      }
    }
    merge.close();

    Long previous = null;
    for (Long index : idCollector) {
      if (previous != null) {
        assertTrue(previous <= index);
      }
      previous = index;
    }

  }
}
