package edu.washington.escience.myria.operator.network.partition;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.storage.TupleBatch;

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
  private final SingleFieldHashPartitionFunction[] pfs;

  /**
   * mappings from cells to partitions.
   */
  private final int[][] cellPartition;

  /**
   * 
   * @param numCells number of cells.
   * @param cellPartition mappings from cells to partitions.
   * @param hypercubeDimensions the sizes of each dimension of the hypercube.
   * @param hashedColumns which fields are hashed.
   * @param mappedHCDimensions mapped hypercube dimensions of hashed columns.
   * 
   */
  public MFMDHashPartitionFunction(final int numCells, final int[][] cellPartition, final int[] hypercubeDimensions,
      final int[] hashedColumns, final int[] mappedHCDimensions) {
    super(numCells);
    this.cellPartition = cellPartition;
    pfs = new SingleFieldHashPartitionFunction[hashedColumns.length];
    for (int i = 0; i < hashedColumns.length; ++i) {
      Preconditions.checkPositionIndex(hashedColumns[i], hypercubeDimensions.length);
      Preconditions.checkArgument(hashedColumns.length == mappedHCDimensions.length,
          "hashedColumns must have the same arity as mappedHCDimensions");
      pfs[i] = new SingleFieldHashPartitionFunction(hypercubeDimensions[mappedHCDimensions[i]], hashedColumns[i],
          mappedHCDimensions[i]);
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
