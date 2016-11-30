package edu.washington.escience.myria.api.encoding;


import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.operator.agg.SketchOperator;

import javax.annotation.Nonnull;

/**
 * Created by Liang Luo Local on 11/29/2016.
 */
public class SketchOperatorEncoding extends UnaryOperatorEncoding<SketchOperator> {
    @Override
    public SketchOperator construct(@Nonnull QueryConstruct.ConstructArgs args) throws MyriaApiException {
        return new SketchOperator(null);
    }
}
