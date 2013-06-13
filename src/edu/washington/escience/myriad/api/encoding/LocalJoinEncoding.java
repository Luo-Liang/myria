package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.operator.LocalJoin;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.Server;

public class LocalJoinEncoding extends OperatorEncoding<LocalJoin> {
  public String argChild1;
  public String argChild2;
  public int[] argColumns1;
  public int[] argColumns2;
  public int[] argSelect1;
  public int[] argSelect2;
  private static final List<String> requiredArguments = ImmutableList.of("argChild1", "argChild2", "argColumns1",
      "argColumns2", "argSelect1", "argSelect2");

  @Override
  public void connect(final Operator current, final Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild1), operators.get(argChild2) });
  }

  @Override
  public LocalJoin construct(Server server) {
    return new LocalJoin(null, null, argColumns1, argColumns2, argSelect1, argSelect2);
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}