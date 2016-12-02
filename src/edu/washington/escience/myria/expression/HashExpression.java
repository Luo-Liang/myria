
package edu.washington.escience.myria.expression;

        import java.io.IOException;
        import java.io.PrintWriter;
        import java.util.Set;

        import com.google.common.collect.ImmutableSet;

        import edu.washington.escience.myria.Type;
        import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Divide two operands in an expression tree. The return value is of type {@link Type.INT_TYPE} if both operands are
 * also INTs, and of type {@link Type.LONG_TYPE} otherwise.
 */
public class HashExpression extends BinaryExpression {

    /***/
    private static final long serialVersionUID = 1L;

    /**
     * This is not really unused, it's used automagically by Jackson deserialization.
     */
    @SuppressWarnings("unused")
    private HashExpression() {}

    /**
     * hash the value.
     *
     * @param left the left operand.
     * @param right the right operand.
     */
    public HashExpression(final ExpressionOperator left, final ExpressionOperator right) {
        super(left, right);
    }


    @Override
    public Type getOutputType(final ExpressionOperatorParameter parameters) {
        return Type.INT_TYPE;
    }

    @Override
    public String getJavaString(final ExpressionOperatorParameter parameters) {
        return new StringBuilder("com.google.common.hash.Hashing.murmur3_128( (int)(" )
                .append(getLeft().getJavaString(parameters))
                .append(")).newHasher().putObject(")
                .append(getRight().getJavaString(parameters))
                .append(", edu.washington.escience.myria.util.TypeFunnel.INSTANCE).hash().asInt()")
                .toString();
    }
}

