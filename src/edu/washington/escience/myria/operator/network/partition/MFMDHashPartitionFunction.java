package edu.washington.escience.myria.operator.network.partition;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.MyriaArrayUtils;

/**
 * Multiple field multiple dimension hash partition function for HyperCubeJoin.
 * 
 */
public final class MFMDHashPartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Partition functions on different dimensions.
   */
  private final HashPartitionFunction[] pfs;

  /** mappings from cells to destinations. */
  @JsonProperty
  public final int[][] cellPartition;

  /** the sizes of each dimension of the hypercube. */
  @JsonProperty
  public final int[] hypercubeDimensions;

  /** which fields are hashed/ */
  @JsonProperty
  public final int[] hashedColumns;

  /** mapped hypercube dimensions of hashed columns */
  @JsonProperty
  public final int[] mappedHCDimensions;

  /**
   * @param cellPartition mappings from cells to partitions.
   * @param hypercubeDimensions the sizes of each dimension of the hypercube.
   * @param hashedColumns which fields are hashed.
   * @param mappedHCDimensions mapped hypercube dimensions of hashed columns.
   * 
   */
  public MFMDHashPartitionFunction(@Nonnull @JsonProperty("cellPartition") final int[][] cellPartition,
      @Nonnull @JsonProperty("hypercubeDimensions") final int[] hypercubeDimensions,
      @Nonnull @JsonProperty("hashedColumns") final int[] hashedColumns,
      @Nonnull @JsonProperty("mappedHCDimensions") final int[] mappedHCDimensions) {
    super(null);
    int[] arr = MyriaArrayUtils.arrayFlattenThenSort(cellPartition);
    for (int i = 0; i < arr.length; i++) {
      Preconditions.checkArgument(arr[i] == i, "invalid cell partition");
    }
    setNumDestinations(arr.length);

    this.cellPartition = cellPartition;
    this.hypercubeDimensions = hypercubeDimensions;
    this.hashedColumns = hashedColumns;
    this.mappedHCDimensions = mappedHCDimensions;
    pfs = new HashPartitionFunction[hashedColumns.length];
    for (int i = 0; i < hashedColumns.length; ++i) {
      Preconditions.checkPositionIndex(hashedColumns[i], hypercubeDimensions.length);
      Preconditions.checkArgument(hashedColumns.length == mappedHCDimensions.length,
          "hashedColumns must have the same arity as mappedHCDimensions");
      pfs[i] = new HashPartitionFunction(hypercubeDimensions[mappedHCDimensions[i]], new int[] { hashedColumns[i] });
      pfs[i].setSeedIndex(mappedHCDimensions[i]);
    }
  }

  @Override
  public int[] distribute(@Nonnull final TupleBatch tb, final int row) {
    int p = 0;
    for (int i = 0; i < pfs.length - 1; i++) {
      p = (p + pfs[i].distribute(tb, row)[0]) * pfs[i + 1].numDestinations();
    }
    p = p + pfs[pfs.length - 1].distribute(tb, row)[0];
    return cellPartition[p];
  }
}
