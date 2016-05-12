package edu.washington.escience.myria.operator.network.partition;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.storage.TupleBatch;

/**
 * put all tuples into one partition.
 */
public final class BroadcastPartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** Always returns all destinations. */
  private int[] p = null;

  /**
   * @param numDestinations number of destinations.
   */
  public BroadcastPartitionFunction(@Nullable @JsonProperty("numDestinations") final Integer numDestinations) {
    super(numDestinations);
    p = new int[numDestinations()];
    for (int i = 0; i < numDestinations(); ++i) {
      p[i] = i;
    }
  }

  @Override
  public int[] distribute(@Nonnull final TupleBatch tb, final int row) {
    return p;
  }
}
