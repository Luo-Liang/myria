package edu.washington.escience.myria.api.encoding;

//import edu.washington.escience.myria.MyriaCommit;

final class MyriaCommit {

  /** Utility class cannot be constructed. */
  private MyriaCommit() {}

  /** The git commit id at build time. */
  public static final String COMMIT = "MYRIALIANGBRANCH";

  /** The git branch at build time. */
  public static final String BRANCH = "MASTER";
}

/**
 * Version info about the source code at build time.
 */
public class VersionEncoding {
  /** The git commit id at build time. */
  public final String commit = MyriaCommit.COMMIT;
  /** The git branch at build time. */
  public final String branch = MyriaCommit.BRANCH;
}
