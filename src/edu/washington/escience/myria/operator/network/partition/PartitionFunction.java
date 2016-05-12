package edu.washington.escience.myria.operator.network.partition;

import java.io.Serializable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.storage.TupleBatch;

/**
 * The ShuffleProducer class uses an instance of the PartitionFunction class to decide which worker a tuple should be
 * routed to.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @Type(value = BroadcastPartitionFunction.class, name = "Broadcast"),
    @Type(value = RoundRobinPartitionFunction.class, name = "RoundRobin"),
    @Type(value = SinglePartitionFunction.class, name = "OnePartition"),
    @Type(value = SingleFieldHashPartitionFunction.class, name = "SingleFieldHash"),
    @Type(value = WorkerIdPartitionFunction.class, name = "IdentityHash"),
    @Type(value = MFMDHashPartitionFunction.class, name = "MFMD"),
    @Type(value = MultiFieldHashPartitionFunction.class, name = "MultiFieldHash") })
public abstract class PartitionFunction implements Serializable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The number of destinations. A tuple can be sent to multiple destinations with IDs ranging from 0 to
   * numDestination-1.
   */
  @JsonProperty
  private Integer numDestinations = null;

  /**
   * @param numDestinations see above. Note that this is a {@link Integer} not an {@link int} so that it can properly
   *          handle <code>null</code> values, e.g., in JSON deserialization.
   */
  public PartitionFunction(@Nullable final Integer numDestinations) {
    Preconditions.checkArgument((numDestinations == null) || (numDestinations > 0),
        "numDestinations argument must be null or > 0");
    this.numDestinations = numDestinations;
  }

  /**
   * @return the number of destinations.
   */
  public final int numDestinations() {
    return numDestinations;
  }

  /**
   * Returns a list of destination IDs in [0, numDestinations) that the tuple should be sent to.
   * 
   * @param tb the tuple batch.
   * @param row the row index of the tuple to be partitioned.
   * @return a list of destination IDs that the tuple should go.
   * 
   */
  public abstract int[] distribute(@Nonnull final TupleBatch tb, final int row);

  /**
   * Set the number of output destinations.
   * 
   * @param numDestinations the number of output destinations. Must be greater than 0.
   */
  public final void setNumDestinations(final int numDestinations) {
    Preconditions.checkArgument(numDestinations > 0, "numDestinations must be > 0");
    this.numDestinations = numDestinations;
  }
}
