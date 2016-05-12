package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.network.EOSController;
import edu.washington.escience.myria.util.MyriaUtils;

public class EOSControllerEncoding extends AbstractProducerEncoding<EOSController> {

  @Override
  public EOSController construct(final ConstructArgs args) {
    return new EOSController(null, getRealOperatorIds(), MyriaUtils.integerSetToIntArray(getRealWorkerIds()));
  }

}
