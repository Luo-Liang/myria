package edu.washington.escience.myria.operator.network.partition;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.HashUtils;

/**
 * The default implementation of the partition function.
 * 
 * The partition of a tuple is decided by the hash code of a preset field of the tuple.
 */
public final class SingleFieldHashPartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The index of the partition field.
   */
  @JsonProperty
  private final int index;

  /**
   * The index of the chosen hashcode in <code>HashUtils</code>.
   */
  @JsonProperty
  private final int seedIndex;

  /**
   * @param numDestinations number of destinations.
   * @param index the index of the partition field.
   * @param seedIndex the index of chosen hash seed.
   */
  @JsonCreator
  public SingleFieldHashPartitionFunction(@Nullable @JsonProperty("numDestinations") final Integer numDestinations,
      @JsonProperty(value = "index", required = true) final Integer index,
      @JsonProperty(value = "seedIndex") final Integer seedIndex) {
    super(numDestinations);
    /* TODO(dhalperi) once Jackson actually implements support for required, remove these checks. */
    this.index = java.util.Objects.requireNonNull(index, "missing property index");
    this.seedIndex = MoreObjects.firstNonNull(seedIndex, 0) % HashUtils.NUM_OF_HASHFUNCTIONS;
    Preconditions.checkArgument(this.index >= 0, "SingleFieldHash field index cannot take negative value %s",
        this.index);
  }

  /**
   * @param numDestinations number of destinations.
   * @param index the index of the hash field.
   */
  public SingleFieldHashPartitionFunction(final Integer numDestinations, final Integer index) {
    this(numDestinations, index, 0);
  }

  /**
   * @return the index
   */
  public int getIndex() {
    return index;
  }

  @Override
  public int[] distribute(@Nonnull final TupleBatch tb, final int row) {
    int p = HashUtils.hashValue(tb, index, row, seedIndex) % numDestinations();
    if (p < 0) {
      p = p + numDestinations();
    }
    return new int[] { p };
  }
}
