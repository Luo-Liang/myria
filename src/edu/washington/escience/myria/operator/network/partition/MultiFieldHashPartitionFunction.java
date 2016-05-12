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
public final class MultiFieldHashPartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The indices used for partitioning. */
  @JsonProperty
  private final int[] indexes;

  /**
   * @param numDestinations number of destinations.
   * @param indexes the indices used for partitioning.
   */
  public MultiFieldHashPartitionFunction(@Nullable @JsonProperty("numDestinations") final Integer numDestinations,
      @JsonProperty(value = "indexes", required = true) final int[] indexes) {
    super(numDestinations);
    Objects.requireNonNull(indexes, "indexes");
    Preconditions.checkArgument(indexes.length > 1, "MultiFieldHash requires at least 2 fields to hash");
    this.indexes = indexes;
    for (int i = 0; i < indexes.length; ++i) {
      Preconditions.checkArgument(indexes[i] >= 0, "MultiFieldHash field index %s cannot take negative value %s", i,
          indexes[i]);
    }
  }

  /**
   * @return the field indexes on which tuples will be hash partitioned.
   */
  public int[] getIndexes() {
    return indexes;
  }

  @Override
  public int[] distribute(@Nonnull final TupleBatch tb, final int row) {
    int p = HashUtils.hashSubRow(tb, indexes, row) % numDestinations();
    if (p < 0) {
      p = p + numDestinations();
    }
    return new int[] { p };
  }
}