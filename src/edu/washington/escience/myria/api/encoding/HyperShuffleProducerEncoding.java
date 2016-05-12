package edu.washington.escience.myria.api.encoding;

import javax.ws.rs.core.Response.Status;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.partition.MFMDHashPartitionFunction;
import edu.washington.escience.myria.util.MyriaArrayUtils;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * Producer part of JSON Encoding for HyperCube Join.
 * 
 */
public class HyperShuffleProducerEncoding extends AbstractProducerEncoding<GenericShuffleProducer> {

  @Required
  public int[] hashedColumns;
  @Required
  public int[] mappedHCDimensions;
  @Required
  public int[] hyperCubeDimensions;
  @Required
  public int[][] cellPartition;
  /* TODO: automatically decode the MFMD from JSON, merge with ShuffleProducerEncoding. */

  @Override
  public GenericShuffleProducer construct(final ConstructArgs args) throws MyriaApiException {
    int numCells = 1;
    for (int d : hyperCubeDimensions) {
      numCells = numCells * d;
    }
    MFMDHashPartitionFunction pf = new MFMDHashPartitionFunction(numCells, cellPartition, hyperCubeDimensions,
        hashedColumns, mappedHCDimensions);
    return new GenericShuffleProducer(null, MyriaUtils.getSingleElement(getRealOperatorIds()), MyriaUtils
        .integerSetToIntArray(args.getServer().getRandomWorkers(numCells)), pf);
  }

  @Override
  protected void validateExtra() {
    int[] arr = MyriaArrayUtils.arrayFlattenThenSort(cellPartition);
    for (int i = 0; i < arr.length; i++) {
      if (arr[i] != i) {
        throw new MyriaApiException(Status.BAD_REQUEST, "invalid cell partition");
      }
    }
  }

}
