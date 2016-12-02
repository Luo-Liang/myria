
package edu.washington.escience.myria.expression;

        import java.io.IOException;
        import java.io.PrintWriter;
        import java.util.List;
        import java.util.Set;

        import com.google.common.collect.ImmutableList;
        import com.google.common.collect.ImmutableSet;

        import edu.washington.escience.myria.Type;
        import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Divide two operands in an expression tree. The return value is of type {@link Type.INT_TYPE} if both operands are
 * also INTs, and of type {@link Type.LONG_TYPE} otherwise.
 */
public class HashExpression extends NAryExpression {

    /***/
    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unused")
    public HashExpression() {}

    /**
     * This is not really unused, it's used automagically by Jackson deserialization.
     */

    private HashExpression(final List<ExpressionOperator> children) { super(ImmutableList.copyOf(children));}


    /**
     * hash the value.
     *
     * @param left the left operand.
     * @param right the right operand.
     */



    @Override
    public Type getOutputType(final ExpressionOperatorParameter parameters) {
        return Type.INT_TYPE;
    }

    @Override
    public String getJavaString(final ExpressionOperatorParameter parameters) {
        StringBuilder retval = new StringBuilder("com.google.common.hash.Hashing.murmur3_128( (int)(" )
                .append(getChildren().get(0).getJavaString(parameters))
                .append(")).newHasher()");
        for(int i = 1; i < getChildren().size(); i++){
            retval.append(".putObject(");
            retval.append(getChildren().get(i).getJavaString(parameters));
            retval.append(", edu.washington.escience.myria.util.TypeFunnel.INSTANCE)");
        }
        retval.append(".hash().asInt()");
        return retval.toString();
      /*  return new StringBuilder("com.google.common.hash.Hashing.murmur3_128( (int)(" )
                .append(getChildren().get(0).getJavaString(parameters))
                .append(")).newHasher().putObject(")
                .append(getChildren().get(1).getJavaString(parameters))
                .append(", edu.washington.escience.myria.util.TypeFunnel.INSTANCE).hash().asInt()")
                .toString();*/
    }
}

