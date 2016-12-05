package edu.washington.escience.myria.operator.agg;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnore;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.agg.PrimitiveAggregator.AggregationOp;

import javax.validation.constraints.AssertTrue;

/**
 * An aggregator for a column of primitive type.
 */
public class SingleColumnAggregatorFactory implements AggregatorFactory {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** Which column of the input to aggregate over. */
  @JsonProperty private final int column;
  /** Which aggregate options are requested. See {@link PrimitiveAggregator}. */
  @JsonProperty private final AggregationOp[] aggOps;
  @JsonProperty private final AggregationSketchOption option;
  /**
   * A wrapper for the {@link PrimitiveAggregator} implementations like {@link IntegerAggregator}.
   *
   * @param column which column of the input to aggregate over.
   * @param aggOps which aggregate operations are requested. See {@link PrimitiveAggregator}.
   */
  @JsonIgnore
  public SingleColumnAggregatorFactory(
      final Integer column,
      final AggregationOp... aggOps) {
    this(column, AggregationSketchOption.DoNotSketch, aggOps);
  }

  @JsonCreator
  public SingleColumnAggregatorFactory(
          @JsonProperty(value = "column", required = true) final Integer column,
          @JsonProperty(value = "sketchOption") final AggregationSketchOption option,
          @JsonProperty(value = "aggOps", required = true) final AggregationOp... aggOps) {
    this.column = Objects.requireNonNull(column, "column").intValue();
    this.aggOps = Objects.requireNonNull(aggOps, "aggOps");
    Preconditions.checkArgument(aggOps.length > 0, "no aggregation operators selected");
    for (int i = 0; i < aggOps.length; ++i) {
      Preconditions.checkNotNull(aggOps[i], "aggregation operator %s cannot be null", i);
    }
    if(option==null)
      this.option = AggregationSketchOption.DoNotSketch;
    else
      this.option = option;
  }

  public AggregationSketchOption getSketchOption()
  {
    return option;
  }


  @Override
  public Aggregator get(Schema inputSchema)
  {
    Objects.requireNonNull(inputSchema, "inputSchema");
    Objects.requireNonNull(aggOps, "aggOps");
    String inputName = inputSchema.getColumnName(column);
    Type type = inputSchema.getColumnType(column);
    switch (type) {
      case BOOLEAN_TYPE:
        return new BooleanAggregator(inputName, aggOps, column);
      case DATETIME_TYPE:
        return new DateTimeAggregator(inputName, aggOps, column);
      case DOUBLE_TYPE:
        return new DoubleAggregator(inputName, aggOps, column);
      case FLOAT_TYPE:
        return new FloatAggregator(inputName, aggOps, column);
      case INT_TYPE:
        IntegerAggregator agg = new IntegerAggregator(inputName, aggOps, column, option);
        return agg;
      case LONG_TYPE:
        if(option == AggregationSketchOption.DoNotSketch) {
          if(aggOps[0] == AggregationOp.COUNT)
          {
            throw new IllegalStateException(option + " is the input option to this factory, which is not sketch");
          }
          return new LongAggregator(inputName, aggOps, column);
        }
        else {
          return new IntegerAggregator(inputName, aggOps, column, option);
          //TODO: currently, even if type is long, downgraee it to int if sketch is enabled.
        }
      case STRING_TYPE:
        return new StringAggregator(inputName, aggOps, column);
    }
    throw new IllegalArgumentException("Unknown column type: " + type);
  }
}
