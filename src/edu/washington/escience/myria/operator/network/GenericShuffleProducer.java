package edu.washington.escience.myria.operator.network;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.network.partition.PartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * GenericShuffleProducer, which support json encoding of 1. Broadcast Shuffle 2. One to one Shuffle (Shuffle) 3. Hyper
 * Cube Join Shuffle (HyperJoinShuffle)
 */
public class GenericShuffleProducer extends Producer {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * the partition function.
   */
  private final PartitionFunction partitionFunction;

  /**
   * Shuffle to the operator ID on one worker.
   * 
   * @param child the child who provides data for this producer to distribute.
   * @param operatorID the destination operator ID where the data goes
   * @param workerID the set of destination worker
   * @param pf the partition function
   */
  public GenericShuffleProducer(final Operator child, final ExchangePairID operatorID, final int workerID,
      final PartitionFunction pf) {
    this(child, new ExchangePairID[] { operatorID }, new int[] { workerID }, pf);
  }

  /**
   * Shuffle to the operator ID on multiple workers.
   * 
   * @param child the child who provides data for this producer to distribute.
   * @param operatorID the destination operator ID where the data goes
   * @param workerIDs set of destination workers
   * @param pf the partition function
   */
  public GenericShuffleProducer(final Operator child, final ExchangePairID operatorID, final int[] workerIDs,
      final PartitionFunction pf) {
    this(child, new ExchangePairID[] { operatorID }, workerIDs, pf);
  }

  /**
   * Shuffle to the operator IDs on one workers.
   * 
   * @param child the child who provides data for this producer to distribute.
   * @param operatorIDs destination operators the data goes
   * @param workerID the destination worker
   * @param pf the partition function
   */
  public GenericShuffleProducer(final Operator child, final ExchangePairID[] operatorIDs, final int workerID,
      final PartitionFunction pf) {
    this(child, operatorIDs, new int[] { workerID }, pf);
  }

  /**
   * Shuffle to multiple operator IDs on multiple workers. The most generic constructor.
   * 
   * @param child the child who provides data for this producer to distribute.
   * @param operatorIDs destination operators the data goes
   * @param workerIDs set of destination workers
   * @param pf the partition function
   */
  public GenericShuffleProducer(final Operator child, final ExchangePairID[] operatorIDs, final int[] workerIDs,
      final PartitionFunction pf) {
    super(child, operatorIDs, workerIDs);
    Preconditions.checkArgument(pf.numDestinations() == getNumOfChannels());
    partitionFunction = pf;
  }

  /**
   * @return return partition function.
   */
  public final PartitionFunction getPartitionFunction() {
    return partitionFunction;
  }

  @Override
  protected final void consumeTuples(final TupleBatch tb) throws DbException {
    distribute(tb, partitionFunction);
  }

  @Override
  protected void childEOS() throws DbException {
    distribute(null, null);
    for (int p = 0; p < getNumOfChannels(); p++) {
      super.channelEnds(p);
    }
  }

  @Override
  protected final void childEOI() throws DbException {
    distribute(TupleBatch.eoiTupleBatch(getSchema()), partitionFunction);
  }
}
