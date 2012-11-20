package io;

import com.esotericsoftware.kryo.io.Output;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.impl.GroupProperties;
import com.hazelcast.nio.DefaultSerializer;
import com.hazelcast.nio.FastByteArrayOutputStream;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;
import java.util.UUID;

import static java.util.UUID.randomUUID;

public class SerializationTest {

   private static final String[] WORDS =
      {"many", "random", "words", "that", "can", "be", "mixed", "        ", "asdf", "   ", "eivind",
         "test    test", "hmm", " ", "  ", "yea", "ok", "<xml/>", "<value>blabla</value>", "æøåÆØÅ"};
   private static final Random RAND = new Random();
   private static final int ENTRY_COUNT = 10000;

   @BeforeClass
   public static void initSerializer() {
      DefaultSerializer.registerSerializer(new ValueSerializer(Compression.SNAPPY));
   }

   @Test
   public void createMapWithManyOperationsOnTime() throws Exception {
      Config config = new Config();
      config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
      config.getMapConfig("testmap").setCacheValue(false);
      config.setProperty(GroupProperties.PROP_LOGGING_TYPE, "none");
      HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

      IMap<Integer, Value> testMap = instance.getMap("testmap");

      long totalCost = 0;
      long timeBefore = System.nanoTime();
      System.out.println("Putting " + ENTRY_COUNT + " entries");
      for (int i = 0; i < ENTRY_COUNT; i++) {
         UUID uuid = randomUUID();
         testMap.put(i, new Value(i, randomString(i), uuid));
         totalCost += testMap.getMapEntry(i).getCost();
         testMap.get(i);
      }
      long timeAfter = System.nanoTime();

      System.out.println("Time: " + (timeAfter - timeBefore) / 1e6 + " ms");
      System.out.println("Total cost: " + totalCost);
      System.out.println("Avg cost: " + totalCost / ENTRY_COUNT);
   }

   @Test
   public void measureSizeOfStringInBytes() throws Exception {
      String testStr = "eivind@<>/\"'-;%æøåÆØÅ";

      System.out.println("Byte count: " + testStr.getBytes().length);
      System.out.println("Byte count ISO-8859-1: " + testStr.getBytes("ISO-8859-1").length);
      System.out.println("Byte count UTF-8: " + testStr.getBytes("UTF-8").length);
   }

   @Test
   public void measureSizeOfIntValues() throws Exception {
      int bufferSize = 100;
      FastByteArrayOutputStream hz = new FastByteArrayOutputStream(bufferSize);
      Output kryo = new Output(bufferSize);

      int value = 1956;
      hz.writeInt(value);
      kryo.writeInt(value, true);

      System.out.println("Hazelcast " + value + ": " + hz.size());
      System.out.println("Kryo " + value + ": " + kryo.total());
   }

   @Test
   public void measureSizeOfLongValues() throws Exception {
      int bufferSize = 100;
      FastByteArrayOutputStream hz = new FastByteArrayOutputStream(bufferSize);
      Output kryo = new Output(bufferSize);

      long value = 100_000_000L;
      hz.writeLong(value);
      kryo.writeLong(value, true);

      System.out.println("Hazelcast " + value + ": " + hz.size());
      System.out.println("Kryo " + value + ": " + kryo.total());
   }

   private String randomString(int number) {
      StringBuilder sb = new StringBuilder(number + ":");
      for (int i = 0; i < 50; i++) {
         sb.append(" ");
         sb.append(randomWord());
      }
      return sb.toString();
   }

   private String randomWord() {
      return WORDS[RAND.nextInt(WORDS.length)];
   }
}
