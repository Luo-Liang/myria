package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.partition.MFMDHashPartitionFunction;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * Producer part of JSON Encoding for HyperCube Join.
 */
public class HyperShuffleProducerEncoding extends AbstractProducerEncoding<GenericShuffleProducer> {

  /** the partition function. */
  @Required
  public MFMDHashPartitionFunction pf;

  @Override
  public GenericShuffleProducer construct(final ConstructArgs args) throws MyriaApiException {
    return new GenericShuffleProducer(null, MyriaUtils.getSingleElement(getRealOperatorIds()), MyriaUtils
        .integerSetToIntArray(args.getServer().getRandomWorkers(pf.numDestinations())), pf);
    /* TODO: get a deterministic subset of workers if we need to remove redundant MFMD shuffle in RACO */
  }
}
