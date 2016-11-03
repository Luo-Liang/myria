package edu.washington.escience.myria.util;

import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.storage.ReadableTable;
import org.joda.time.DateTime;

/**
 * A utility class for hashing tuples and parts of tuples.
 */
public final class HashUtils {
  /**
   * Utility classes have no constructors.
   */
  private HashUtils() {}

  /**
   * picked from http://planetmath.org/goodhashtableprimes.
   */
  private static final int[] SEEDS = {
          243,
          402653189,
          24593,
          786433,
          3145739,
          12289,
          49157,
          6151,
          98317,
          1572869,
          53,
          97,
          193,
          389,
          769,
          1543,
          3079,
          49157,
          98317,
          196613,
          393241,
          6291469,
          12582917,
          25165843,
          50331653,
          100663319,
          201326611,
          805306457,
          1610612741
  };

  /**
   * The hash functions.
   */
  private static final HashFunction[] HASH_FUNCTIONS = {
    Hashing.murmur3_128(SEEDS[0]),
    Hashing.murmur3_128(SEEDS[1]),
    Hashing.murmur3_128(SEEDS[2]),
    Hashing.murmur3_128(SEEDS[3]),
    Hashing.murmur3_128(SEEDS[4]),
    Hashing.murmur3_128(SEEDS[5]),
    Hashing.murmur3_128(SEEDS[6]),
    Hashing.murmur3_128(SEEDS[7]),
    Hashing.murmur3_128(SEEDS[8]),
    Hashing.murmur3_128(SEEDS[9]),
    Hashing.murmur3_128(SEEDS[10]),
    Hashing.murmur3_128(SEEDS[11]),
    Hashing.murmur3_128(SEEDS[12]),
    Hashing.murmur3_128(SEEDS[13]),
    Hashing.murmur3_128(SEEDS[14]),
    Hashing.murmur3_128(SEEDS[15]),
    Hashing.murmur3_128(SEEDS[16]),
    Hashing.murmur3_128(SEEDS[17]),
    Hashing.murmur3_128(SEEDS[18]),
    Hashing.murmur3_128(SEEDS[19]),
    Hashing.murmur3_128(SEEDS[20]),
    Hashing.murmur3_128(SEEDS[21]),
    Hashing.murmur3_128(SEEDS[22]),
    Hashing.murmur3_128(SEEDS[23]),
    Hashing.murmur3_128(SEEDS[24]),
    Hashing.murmur3_128(SEEDS[25]),
          Hashing.murmur3_128(SEEDS[26]),
          Hashing.murmur3_128(SEEDS[27]),
          Hashing.murmur3_128(SEEDS[28]),
  };

  /**
   * Size of the hash function pool.
   */
  public static final int NUM_OF_HASHFUNCTIONS = 26;

  /**
   * Compute the hash code of all the values in the specified row, in column order.
   *
   * @param table the table containing the values
   * @param row   the row to be hashed
   * @return the hash code of all the values in the specified row, in column order
   */
  public static int hashRow(final ReadableTable table, final int row) {
    Hasher hasher = HASH_FUNCTIONS[0].newHasher();
    for (int i = 0; i < table.numColumns(); ++i) {
      addValue(hasher, table, i, row);
    }
    return hasher.hash().asInt();
  }

  public static int[] hashValueFamily(
      final ReadableTable table, final int column, final int row, int requestedHashCount) {
    int[] result = new int[requestedHashCount];
    for (int i = 0; i < requestedHashCount; i++) {
      Hasher hasher = HASH_FUNCTIONS[i].newHasher();
      addValue(hasher, table, column, row);
      result[i] = hasher.hash().asInt();
    }
    return result;
  }

  public static int[] hashValueFamily(final Object key, final Type type, int requestedHashCount) {
    int[] result = new int[requestedHashCount];
    for (int i = 0; i < requestedHashCount; i++) {
      Hasher hasher = HASH_FUNCTIONS[i].newHasher();
      addValue(hasher, type, key);
      result[i] = hasher.hash().asInt();
    }
    return result;
  }

  /**
   * Compute the hash code of the value in the specified column and row of the given table.
   *
   * @param table  the table containing the values to be hashed
   * @param column the column containing the value to be hashed
   * @param row    the row containing the value to be hashed
   * @return the hash code of the specified value
   */
  public static int hashValue(final ReadableTable table, final int column, final int row) {
    Hasher hasher = HASH_FUNCTIONS[0].newHasher();
    addValue(hasher, table, column, row);
    return hasher.hash().asInt();
  }

  /**
   * Compute the hash code of the value in the specified column and row of the given table with specific hashcode.
   *
   * @param table     the table containing the values to be hashed
   * @param column    the column containing the value to be hashed
   * @param row       the row containing the value to be hashed
   * @param seedIndex the index of the chosen hashcode
   * @return hash code of the specified seed
   */
  public static int hashValue(
      final ReadableTable table, final int column, final int row, final int seedIndex) {
    Preconditions.checkPositionIndex(seedIndex, NUM_OF_HASHFUNCTIONS);
    Hasher hasher = HASH_FUNCTIONS[seedIndex].newHasher();
    addValue(hasher, table, column, row);
    return hasher.hash().asInt();
  }

  /**
   * Compute the hash code of the specified columns in the specified row of the given table.
   *
   * @param table       the table containing the values to be hashed
   * @param hashColumns the columns to be hashed. Order matters
   * @param row         the row containing the values to be hashed
   * @return the hash code of the specified columns in the specified row of the given table
   */
  public static int[] hashSubRowFamily(final ReadableTable table, final int[] hashColumns, final int row, final int requestedHashCount) {
    Objects.requireNonNull(table, "table");
    Objects.requireNonNull(hashColumns, "hashColumns");
    int[] result = new int[requestedHashCount];
    for(int i = 0; i < requestedHashCount; i++)
    {
      Hasher hasher = HASH_FUNCTIONS[0].newHasher();
      for (int column : hashColumns)
      {
        addValue(hasher, table, column, row);
      }
      result[i] = hasher.hash().asInt();
    }
    return result;
  }

  /**
   * Compute the hash code of the specified columns in the specified row of the given table.
   *
   * @param table       the table containing the values to be hashed
   * @param hashColumns the columns to be hashed. Order matters
   * @param row         the row containing the values to be hashed
   * @return the hash code of the specified columns in the specified row of the given table
   */
  public static int hashSubRow(final ReadableTable table, final int[] hashColumns, final int row) {
    Objects.requireNonNull(table, "table");
    Objects.requireNonNull(hashColumns, "hashColumns");
    Hasher hasher = HASH_FUNCTIONS[0].newHasher();
    for (int column : hashColumns) {
      addValue(hasher, table, column, row);
    }
    return hasher.hash().asInt();
  }

  /**
   * Add the value at the specified row and column to the specified hasher.
   *
   * @param hasher the hasher
   * @param table  the table containing the value
   * @param column the column containing the value
   * @param row    the row containing the value
   * @return the hasher
   */
  private static Hasher addValue(
      final Hasher hasher, final ReadableTable table, final int column, final int row) {
    return addValue(hasher, table.asColumn(column), row);
  }

  /**
   * Add the value at the specified row and column to the specified hasher.
   *
   * @param hasher the hasher
   * @param column the column containing the value
   * @param row    the row containing the value
   * @return the hasher
   */
  private static Hasher addValue(final Hasher hasher, final ReadableColumn column, final int row) {
    //switch (column.getType()) {
    //    case BOOLEAN_TYPE:
    //        return hasher.putBoolean(column.getBoolean(row));
    //    case DATETIME_TYPE:
    //        return hasher.putObject(column.getDateTime(row), TypeFunnel.INSTANCE);
    //    case DOUBLE_TYPE:
    //        return hasher.putDouble(column.getDouble(row));
    //    case FLOAT_TYPE:
    //        return hasher.putFloat(column.getFloat(row));
    //    case INT_TYPE:
    //        return hasher.putInt(column.getInt(row));
    //    case LONG_TYPE:
    //        return hasher.putLong(column.getLong(row));
    //    case STRING_TYPE:
    //        return hasher.putObject(column.getString(row), TypeFunnel.INSTANCE);
    //}
    //throw new UnsupportedOperationException("Hashing a column of type " + column.getType());
    return addValue(hasher, column.getType(), column.getObject(row));
  }

  /**
   * Add the value at the specified row and column to the specified hasher.
   *
   * @param hasher the hasher
   * @param type the type of value
   * @param value the boxed value
   * @return the hasher
   */
  private static Hasher addValue(final Hasher hasher, final Type type, Object value) {
    switch (type) {
      case BOOLEAN_TYPE:
        return hasher.putBoolean((boolean) value);
      case DATETIME_TYPE:
        return hasher.putObject(value, TypeFunnel.INSTANCE);
      case DOUBLE_TYPE:
        return hasher.putDouble((double) value);
      case FLOAT_TYPE:
        return hasher.putFloat((float) value);
      case INT_TYPE:
        return hasher.putInt((int) value);
      case LONG_TYPE:
        return hasher.putLong((long) value);
      case STRING_TYPE:
        return hasher.putObject(value, TypeFunnel.INSTANCE);
    }
    throw new UnsupportedOperationException("Hashing a column of type " + type);
  }
}
