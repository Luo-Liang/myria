package edu.washington.escience.myria.operator.network.partition;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonCreator;

import edu.washington.escience.myria.storage.TupleBatch;

/**
 * A partition function that simply sends one tuple to each output in turn.
 * 
 * 
 */
public final class RoundRobinPartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** */
  @JsonCreator
  public RoundRobinPartitionFunction() {
    super();
  }

  @Override
  public int[] distribute(@Nonnull final TupleBatch tb, final int row) {
    return new int[] { row % numDestinations() };
  }
}
