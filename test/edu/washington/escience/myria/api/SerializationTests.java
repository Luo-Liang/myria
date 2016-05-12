package edu.washington.escience.myria.api;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import edu.washington.escience.myria.operator.network.partition.HashPartitionFunction;
import edu.washington.escience.myria.operator.network.partition.PartitionFunction;
import edu.washington.escience.myria.operator.network.partition.RoundRobinPartitionFunction;

public class SerializationTests {

  public static ObjectMapper mapper;

  @BeforeClass
  public static void setUp() {
    mapper = MyriaJsonMapperProvider.getMapper();
  }

  @Test
  public void testPartitionFunction() throws Exception {
    /* Setup */
    ObjectReader reader = mapper.reader(PartitionFunction.class);
    PartitionFunction pf;
    String serialized;
    PartitionFunction deserialized;

    /* Single field hash */
    pf = new HashPartitionFunction(5, 3);
    serialized = mapper.writeValueAsString(pf);
    deserialized = reader.readValue(serialized);
    assertEquals(pf.getClass(), deserialized.getClass());
    assertEquals(5, deserialized.numDestinations());
    HashPartitionFunction pfSFH = (HashPartitionFunction) deserialized;
    assertEquals(3, pfSFH.getIndexes()[0]);

    /* Multi-field hash */
    int[] multiFieldIndex = new int[] { 3, 4, 2 };
    pf = new HashPartitionFunction(5, multiFieldIndex);
    serialized = mapper.writeValueAsString(pf);
    deserialized = reader.readValue(serialized);
    assertEquals(pf.getClass(), deserialized.getClass());
    assertEquals(5, deserialized.numDestinations());
    HashPartitionFunction pfMFH = (HashPartitionFunction) deserialized;
    assertArrayEquals(multiFieldIndex, pfMFH.getIndexes());

    /* RoundRobin */
    pf = new RoundRobinPartitionFunction(5);
    serialized = mapper.writeValueAsString(pf);
    deserialized = reader.readValue(serialized);
    assertEquals(pf.getClass(), deserialized.getClass());
    assertEquals(5, deserialized.numDestinations());
  }

  @Test
  public void testPartitionFunctionWithNullNumPartitions() throws Exception {
    /* Setup */
    ObjectReader reader = mapper.reader(PartitionFunction.class);
    PartitionFunction pf;
    String serialized;
    PartitionFunction deserialized;

    /* Single field hash, as one representative */
    pf = new HashPartitionFunction(null, 3);
    serialized = mapper.writeValueAsString(pf);
    deserialized = reader.readValue(serialized);
    assertEquals(pf.getClass(), deserialized.getClass());
    assertEquals(3, ((HashPartitionFunction) deserialized).getIndexes()[0]);
  }

}
