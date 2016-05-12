package edu.washington.escience.myria.operator.network.partition;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.storage.TupleBatch;

/**
 * 
 * put all tuples into one partition.
 * 
 */
public final class SinglePartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** always returns one partition. */
  private final int[] p = { 0 };

  /** Constructor. */
  public SinglePartitionFunction() {
    super();
  }

  @Override
  public int[] distribute(@Nonnull final TupleBatch tb, final int row) {
    return p;
  }

}
