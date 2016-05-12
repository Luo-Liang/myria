package edu.washington.escience.myria.operator.network.partition;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.storage.TupleBatch;

/**
 * A partition function that simply sends one tuple to each output in turn.
 * 
 * 
 */
public final class RoundRobinPartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * @param numDestinations the number of destinations.
   */
  @JsonCreator
  public RoundRobinPartitionFunction(@Nullable @JsonProperty("numDestinations") final Integer numDestinations) {
    super(numDestinations);
  }

  @Override
  public int[] distribute(@Nonnull final TupleBatch tb, final int row) {
    return new int[] { row % numDestinations() };
  }
}
