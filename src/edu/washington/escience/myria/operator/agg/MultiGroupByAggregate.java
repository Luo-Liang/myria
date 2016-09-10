package edu.washington.escience.myria.operator.agg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.gs.collections.impl.list.mutable.primitive.IntArrayList;
import com.gs.collections.impl.map.mutable.primitive.IntObjectHashMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.UnaryOperator;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.storage.TupleBuffer;
import edu.washington.escience.myria.storage.TupleUtils;
import edu.washington.escience.myria.util.HashUtils;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max, min). This variant supports aggregates over
 * multiple columns, group by multiple columns.
 *
 * @see Aggregate
 * @see SingleGroupByAggregate
 */
public final class MultiGroupByAggregate extends UnaryOperator {
  /** logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(StatefulUserDefinedAggregator.class);

  /** Java requires this. **/
  private static final long serialVersionUID = 1L;

  /** Holds the distinct grouping keys. */
  private transient TupleBuffer groupKeys;
  /** Final group keys. */
  private List<TupleBatch> groupKeyList;
  /** Holds the corresponding aggregation state for each group key in {@link #groupKeys}. */
  private transient List<Object[]> aggStates;
  // private transient List<List<Object>> tbgroupsState;
  private transient List<List<TupleBatch>> tbgroupState;
  private transient List<BitSet> bs;
  /** Maps the hash of a grouping key to a list of indices in {@link #groupKeys}. */
  private transient IntObjectHashMap<IntArrayList> groupKeyMap;
  /** The schema of the columns indicated by the group keys. */
  private Schema groupSchema;
  /** The schema of the aggregation result. */
  private Schema aggSchema;

  /** Factories to make the Aggregators. **/
  private final AggregatorFactory[] factories;
  /** The actual Aggregators. **/
  private Aggregator[] aggregators;
  /** Group fields. **/
  private final int[] gfields;
  /** An array [0, 1, .., gfields.length-1] used for comparing tuples. */
  private final int[] grpRange;

  private Schema inputSchema;

  /**
   * Groups the input tuples according to the specified grouping fields, then produces the specified aggregates.
   *
   * @param child The Operator that is feeding us tuples.
   * @param gfields The columns over which we are grouping the result.
   * @param factories The factories that will produce the {@link Aggregator}s for each group..
   */
  public MultiGroupByAggregate(@Nullable final Operator child, final int[] gfields,
      final AggregatorFactory... factories) {
    super(child);

    this.gfields = Objects.requireNonNull(gfields, "gfields");
    this.factories = Objects.requireNonNull(factories, "factories");
    Preconditions.checkArgument(gfields.length > 1, "to use MultiGroupByAggregate, must group over multiple fields");
    Preconditions.checkArgument(factories.length != 0, "to use MultiGroupByAggregate, must specify some aggregates");
    LOGGER.info("gfields: " + gfields.length);
    // gfileds is the number of columns to group by
    // but factory is only one since one aggregation
    for (AggregatorFactory factorie : factories) {
      LOGGER.info("factory name " + factorie.getClass().getName());
    }
    grpRange = new int[gfields.length];
    for (int i = 0; i < gfields.length; ++i) {
      grpRange[i] = i;
    }
    groupKeyList = null;

  }

  @Override
  protected void cleanup() throws DbException {
    groupKeys = null;
    aggStates = null;
    groupKeyMap = null;
    groupKeyList = null;
  }

  /**
   * Returns the next tuple. If there is a group by field, then the first field is the field by which we are grouping,
   * and the second field is the result of computing the aggregate, If there is no group by field, then the result tuple
   * should contain one field representing the result of the aggregate. Should return null if there are no more tuples.
   *
   * @throws DbException if any error occurs.
   * @return result TB.
   * @throws IOException
   */
  @Override
  protected TupleBatch fetchNextReady() throws DbException, IOException {
    final Operator child = getChild();

    if (child.eos()) {
      return getResultBatch();
    }

    TupleBatch tb = child.nextReady();
    int tbnum = 0;
    while (tb != null) {
      for (int row = 0; row < tb.numTuples(); ++row) {
        // get row hash
        // gettb hash
        // int tbhash = HashUtils.hashRow(table, row)
        int rowHash = HashUtils.hashSubRow(tb, gfields, row);
        // have we seen this row before?
        IntArrayList hashMatches = groupKeyMap.get(rowHash);
        if (hashMatches == null) {
          // LOGGER.info("hashMatches is null");
          hashMatches = newKey(rowHash);
          newGroup(tb, row, hashMatches, tbnum);
          continue;
        }
        boolean found = false;
        for (int i = 0; i < hashMatches.size(); i++) {
          int value = hashMatches.get(i);
          LOGGER.info("index for aggstate " + value);
          if (TupleUtils.tupleEquals(tb, gfields, row, groupKeys, grpRange, value)) {
            LOGGER.info("Agg state size " + aggStates.size() + " Tbgroupstate " + tbgroupState.size());
            addBitSet(row, tbnum, value, tb.numTuples(), false);
            // updateGroup(tb, row, aggStates.get(value), tbgroupsState.get(value), bs.get(value));
            found = true;
            break;
          }
        }

        if (!found) {
          newGroup(tb, row, hashMatches, tbnum);
        }

        Preconditions.checkState(groupKeys.numTuples() == aggStates.size());
      }
      updateGroups(tb, tbnum);
      tbnum++;
      tb = child.nextReady();
    }

    /*
     * We know that child.nextReady() has returned <code>null</code>, so we have processed all tuple we can. Child is
     * either EOS or we have to wait for more data.
     */
    if (child.eos()) {
      return getResultBatch();
    }

    return null;
  }

  private void updateGroups(final TupleBatch tb, final int numTupleBatch) {
    LOGGER.info("tbgroupState size = " + tbgroupState.size());
    LOGGER.info("bs size = " + bs.size());
    LOGGER.info("updating groups for tuplebatch number " + numTupleBatch);
    for (int j = 0; j < bs.size(); j++) {
      LOGGER.info("bit set list for tuplebatch " + j + "is " + bs.get(j).size());
    }

    for (int i = 0; i < tbgroupState.size(); i++) {
      LOGGER.info("getting group state " + i);
      List<TupleBatch> ltb = tbgroupState.get(i);
      BitSet curBitSet = bs.get(i);
      TupleBatch filteredtb = tb.filter(curBitSet);
      if (filteredtb.numTuples() > 0) {
        LOGGER.info("filtered tuple had stuff");
        ltb.add(filteredtb);
      } else {
        LOGGER.info("filetered tuple was empty");
      }
      bs.get(i).clear();

    }

  }

  private void addBitSet(final int row, final int tbid, final int groupid, final int numTuples,
      final boolean bNewGroup) {
    LOGGER.info("tuplebatch id = " + tbid);
    LOGGER.info("groupid =  " + groupid);
    // if a new group, then add number of bitsets==tbid

    for (int group = 0; group < bs.size(); group++) {
      if (group == groupid) {
        bs.get(group).set(row);
      }
    }

  }

  /**
   * Since row <code>row</code> in {@link TupleBatch} <code>tb</code> does not appear in {@link #groupKeys}, create a
   * new group for it.
   *
   * @param tb the source {@link TupleBatch}
   * @param row the row in <code>tb</code> that contains the new group
   * @param hashMatches the list of all rows in the output {@link TupleBuffer}s that match this hash.
   * @throws DbException if there is an error.
   */
  private void newGroup(final TupleBatch tb, final int row, final IntArrayList hashMatches, final int tbid)
      throws DbException {
    int newIndex = groupKeys.numTuples();
    // LOGGER.info("new Index " + newIndex);
    for (int column = 0; column < gfields.length; ++column) {
      // LOGGER.info("copy value");
      TupleUtils.copyValue(tb, gfields[column], row, groupKeys, column);
      LOGGER.info("group keys now has  " + groupKeys.numTuples() + " tuples");
    }
    hashMatches.add(newIndex);
    Object[] curAggStates = AggUtils.allocateAggStates(aggregators);
    aggStates.add(curAggStates);

    // Allocate a tuple batch list to hold state tuples
    List<TupleBatch> ltb = new ArrayList<TupleBatch>();
    tbgroupState.add(ltb);

    // create a bitset for this tuplebatch

    BitSet curbitSet = new BitSet(tb.numTuples());
    bs.add(curbitSet);

    LOGGER.info(" added new bitsetlist to new group with index: " + newIndex);
    addBitSet(row, tbid, newIndex, tb.numTuples(), true);

    // updateGroup(tb, row, curAggStates, statelt, tokeeplist);

    Preconditions.checkState(groupKeys.numTuples() == aggStates.size(), "groupKeys %s != groupAggs %s", groupKeys
        .numTuples(), aggStates.size());
  }

  /**
   * Called when there is no list yet of which output aggregators match the specified hash. Creates a new int list to
   * store these matches, and insert it into the {@link #groupKeyMap}.
   *
   * @param groupHash the hash of the grouping columns in a tuple
   * @return the new (empty still) int list storing which output aggregators match the specified hash
   */
  private IntArrayList newKey(final int groupHash) {
    IntArrayList matches = new IntArrayList(1);
    groupKeyMap.put(groupHash, matches);
    return matches;
  }

  /**
   * Update the aggregation states with the tuples in the specified row.
   *
   * @param tb the source {@link TupleBatch}
   * @param row the row in <code>tb</code> that contains the new values
   * @param curAggStates the aggregation states to be updated.
   * @throws DbException if there is an error.
   */
  @SuppressWarnings("deprecation")
  private void updateGroup(final TupleBatch tb, final int row, final Object[] curAggStates, final List<Object> state)
      throws DbException {

    for (int agg = 0; agg < aggregators.length; ++agg) {
      if (aggregators[agg] instanceof StatefulUserDefinedAggregator) {
        // copy the tuple to groupagg state - don't calladd
        // Tuple tup = new Tuple(inputSchema);
        // // copy tuple
        // for (int i = 0; i < tb.numColumns(); i++) {
        // tup.set(i, tb.getObject(i, row));
        // }
        // state.add(tup);
        // LOGGER.info("length of state: " + state.size());
        // BitSet toKeep = new BitSet(tb.numTuples());
        // LOGGER.info("bitset not set? "+toKeep.toString());
        // LOGGER.info("row id is"+ row);
        // toKeep.set(row);
        // LOGGER.info("did the biset set? "+toKeep.toString());
        // bslist.add(toKeep);

        // aggregators[agg].add(tb, curAggStates[agg]);
      } else {
        aggregators[agg].addRow(tb, row, curAggStates[agg]);
      }

    }
  }

  /**
   * @return A batch's worth of result tuples from this aggregate.
   * @throws DbException if there is an error.were you thinking of
   * @throws IOException
   */
  private TupleBatch getResultBatch() throws DbException, IOException {
    Preconditions.checkState(getChild().eos(), "cannot extract results from an aggregate until child has reached EOS");
    if (groupKeyList == null) {
      groupKeyList = Lists.newLinkedList(groupKeys.finalResult());
      groupKeys = null;
    }

    if (groupKeyList.isEmpty()) {
      return null;
    }

    TupleBatch curGroupKeys = groupKeyList.remove(0);
    TupleBatchBuffer curGroupAggs = new TupleBatchBuffer(aggSchema);
    LOGGER.info("number of aggStates: " + aggStates.size());
    LOGGER.info("number of tbgroupsStates: " + tbgroupState.size());
    LOGGER.info("number of Bitsets: " + bs.size());
    // for (int j = 0; j < bs.size(); j++) {
    // BitSet a = bs.get(j);
    // LOGGER.info("size of " + j + "th bistset list is - " + a.size());
    // LOGGER.info("Printing bitset ");
    // for (int r = 0; r < a.size(); r++) {
    // LOGGER.info("printing bitset: " + a.toString());
    // }
    // }
    for (int row = 0; row < curGroupKeys.numTuples(); ++row) {

      Object[] rowAggs = aggStates.get(row);
      List<TupleBatch> lt = tbgroupState.get(row);
      LOGGER.info("group row: " + row);
      int curCol = 0;
      for (int agg = 0; agg < aggregators.length; ++agg) {
        // here check if the type is statefule agg and then pass the tuplelist with the
        // right tuples -maybe call add here with tuplelist
        // then call get result for the agg -- this will be called for each group
        // ensure previous list of uples is cleared before next set is sent in...
        if (aggregators[agg].getClass().getName().equals(StatefulUserDefinedAggregator.class.getName())) {

          aggregators[agg].add(lt, rowAggs[agg]);
          aggregators[agg].getResult(curGroupAggs, curCol, rowAggs[agg]);
        } else {
          aggregators[agg].getResult(curGroupAggs, curCol, rowAggs[agg]);
        }
        curCol += aggregators[agg].getResultSchema().numColumns();
      }
    }
    TupleBatch aggResults = curGroupAggs.popAny();
    Preconditions.checkState(curGroupKeys.numTuples() == aggResults.numTuples(),
        "curGroupKeys size %s != aggResults size %s", curGroupKeys.numTuples(), aggResults.numTuples());

    /* Note: as of Java7 sublists of sublists do what we want -- the sublists are at most one deep. */
    aggStates = aggStates.subList(curGroupKeys.numTuples(), aggStates.size());
    return new TupleBatch(getSchema(), ImmutableList.<Column<?>> builder().addAll(curGroupKeys.getDataColumns()).addAll(
        aggResults.getDataColumns()).build());
  }

  /**
   * The schema of the aggregate output. Grouping fields first and then aggregate fields. The aggregate
   *
   * @return the resulting schema
   */
  @Override
  protected Schema generateSchema() {
    Operator child = getChild();
    if (child == null) {
      return null;
    }
    Schema inputSchema = child.getSchema();
    if (inputSchema == null) {
      return null;
    }

    groupSchema = inputSchema.getSubSchema(gfields);

    /* Build the output schema from the group schema and the aggregates. */
    final ImmutableList.Builder<Type> aggTypes = ImmutableList.<Type> builder();
    final ImmutableList.Builder<String> aggNames = ImmutableList.<String> builder();

    try {

      if (pyFuncReg != null) {
        for (Aggregator agg : AggUtils.allocateAggs(factories, getChild().getSchema(), pyFuncReg)) {
          Schema curAggSchema = agg.getResultSchema();
          aggTypes.addAll(curAggSchema.getColumnTypes());
          aggNames.addAll(curAggSchema.getColumnNames());
        }

      } else {
        for (Aggregator agg : AggUtils.allocateAggs(factories, inputSchema, null)) {

          Schema curAggSchema = agg.getResultSchema();
          aggTypes.addAll(curAggSchema.getColumnTypes());
          aggNames.addAll(curAggSchema.getColumnNames());
        }
      }

    } catch (DbException e) {
      throw new RuntimeException("unable to allocate aggregators to determine output schema", e);
    }
    aggSchema = new Schema(aggTypes, aggNames);
    return Schema.merge(groupSchema, aggSchema);
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    Preconditions.checkState(getSchema() != null, "unable to determine schema in init");
    inputSchema = getChild().getSchema();
    LOGGER.info("input schema? " + inputSchema.toString());

    if (pyFuncReg != null) {
      aggregators = AggUtils.allocateAggs(factories, getChild().getSchema(), pyFuncReg);
      LOGGER.info("number of aggs " + aggregators.length);
    } else {
      aggregators = AggUtils.allocateAggs(factories, getChild().getSchema(), null);
    }
    groupKeys = new TupleBuffer(groupSchema);
    aggStates = new ArrayList<>();
    tbgroupState = new ArrayList<>();
    bs = new ArrayList<>();
    groupKeyMap = new IntObjectHashMap<>();
  }
};
