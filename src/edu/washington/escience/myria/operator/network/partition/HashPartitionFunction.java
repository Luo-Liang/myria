package edu.washington.escience.myria.operator.network.partition;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.HashUtils;

/**
 * Implementation that uses multiple fields as the key to hash
 * 
 * The partition of a tuple is decided by the hash code of a group of fields of the tuple.
 */
public final class HashPartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The indices used for partitioning. */
  @JsonProperty
  private final int[] indexes;

  /** The index of the chosen hashcode in <code>HashUtils</code>. */
  @JsonProperty
  private int seedIndex = 0;

  /**
   * @param numDestinations number of destinations.
   * @param indexes the column indices used for partitioning.
   * @param seedIndex the index of the chosen hashcode
   */
  public HashPartitionFunction(@Nullable @JsonProperty("numDestinations") final Integer numDestinations,
      @JsonProperty(value = "indexes", required = true) final int[] indexes) {
    super(numDestinations);
    Objects.requireNonNull(indexes, "indexes");
    Preconditions.checkArgument(indexes.length > 0, "hash function requires at least 1 field to hash");
    this.indexes = indexes;
    for (int i = 0; i < indexes.length; ++i) {
      Preconditions.checkArgument(indexes[i] >= 0, "hash function field index %s cannot take negative value %s", i,
          indexes[i]);
    }
  }

  /**
   * @param seedIndex the index of the chosen hashcode.
   */
  public void setSeedIndex(final int seedIndex) {
    this.seedIndex = seedIndex % HashUtils.NUM_OF_HASHFUNCTIONS;
  }

  /**
   * @param numDestinations number of destinations.
   * @param index the column index used for partitioning.
   */
  public HashPartitionFunction(final Integer numDestinations, final int index) {
    this(numDestinations, new int[] { index });
  }

  /**
   * @return the field indexes on which tuples are hash partitioned.
   */
  public int[] getIndexes() {
    return indexes;
  }

  @Override
  public int[] distribute(@Nonnull final TupleBatch tb, final int row) {
    int p = HashUtils.hashSubRow(tb, indexes, row, seedIndex) % numDestinations();
    if (p < 0) {
      p = p + numDestinations();
    }
    return new int[] { p };
  }
}
