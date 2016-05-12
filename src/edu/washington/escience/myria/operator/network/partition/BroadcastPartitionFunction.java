package edu.washington.escience.myria.operator.network.partition;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.storage.TupleBatch;

/**
 * put all tuples into one partition.
 */
public final class BroadcastPartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** Always returns all destinations. */
  private int[] p = null;

  /** */
  public BroadcastPartitionFunction() {
  }

  @Override
  public void setNumDestinations(final int numDestinations) {
    this.numDestinations = numDestinations;
    p = new int[numDestinations];
    for (int i = 0; i < numDestinations; ++i) {
      p[i] = i;
    }
  }

  @Override
  public int[] distribute(@Nonnull final TupleBatch tb, final int row) {
    return p;
  }
}
