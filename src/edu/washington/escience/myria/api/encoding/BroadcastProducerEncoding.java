package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.partition.BroadcastPartitionFunction;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * JSON wrapper for BroadcastProducer
 */
public class BroadcastProducerEncoding extends AbstractProducerEncoding<GenericShuffleProducer> {

  @Override
  public GenericShuffleProducer construct(final ConstructArgs args) {
    return new GenericShuffleProducer(null, getRealOperatorIds().get(0), MyriaUtils.integerSetToIntArray(
        getRealWorkerIds()), new BroadcastPartitionFunction());
  }

}
