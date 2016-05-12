package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.util.MyriaUtils;

public class CollectProducerEncoding extends AbstractProducerEncoding<CollectProducer> {

  @Override
  public CollectProducer construct(final ConstructArgs args) {
    return new CollectProducer(null, getRealOperatorIds().get(0), MyriaUtils.getSingleElement(getRealWorkerIds()));
  }

}
