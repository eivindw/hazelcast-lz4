package io;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.impl.GroupProperties;
import com.hazelcast.nio.DefaultSerializer;
import com.hazelcast.nio.FastByteArrayInputStream;
import com.hazelcast.nio.FastByteArrayOutputStream;
import com.hazelcast.nio.TypeSerializer;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4Uncompressor;
import org.junit.Test;

import java.io.Serializable;
import java.util.Random;
import java.util.UUID;

import static java.util.UUID.randomUUID;

public class SerializationTest {

   private static final String[] WORDS =
      {"many", "random", "words", "that", "can", "be", "mixed", "        ", "asdf", "   ", "eivind",
       "test    test", "hmm", " ", "  ", "yea", "ok", "<xml/>", "<value>blabla</value>"};
   private static final Random RAND = new Random();
   private static final int ENTRY_COUNT = 10000;

   @Test
   public void createMapWithManyOperationsOnTime() throws Exception {
      DefaultSerializer.registerSerializer(new ValueSerializer(true, true));

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

   private static class Value implements Serializable {
      private int number;
      private String someString;
      private UUID someUUID;

      private Value(int number, String someString, UUID someUUID) {
         this.number = number;
         this.someString = someString;
         this.someUUID = someUUID;
      }

      public int getNumber() {
         return number;
      }

      public String getSomeString() {
         return someString;
      }

      public UUID getSomeUUID() {
         return someUUID;
      }
   }

   private static class ValueSerializer implements TypeSerializer<Value> {

      private final boolean useLZ4;

      private static LZ4Compressor compressor;
      private static LZ4Uncompressor uncompressor;

      private ValueSerializer(boolean useLZ4, boolean useFastCompressor) {
         this.useLZ4 = useLZ4;
         if(useLZ4) {
            if(useFastCompressor) {
               compressor = LZ4Factory.unsafeInstance().fastCompressor();
            } else {
               compressor = LZ4Factory.unsafeInstance().highCompressor();
            }
            uncompressor = LZ4Factory.unsafeInstance().uncompressor();
         }
      }

      public int priority() {
         return 1000;
      }

      public boolean isSuitable(Object o) {
         return o instanceof Value;
      }

      public byte getTypeId() {
         return 10;
      }

      public void write(FastByteArrayOutputStream os, Value value) throws Exception {
         os.writeInt(value.getNumber());
         os.writeLong(value.getSomeUUID().getMostSignificantBits());
         os.writeLong(value.getSomeUUID().getLeastSignificantBits());

         if (useLZ4) {
            final byte[] stringBytes = value.getSomeString().getBytes("UTF-8");
            final int maxLength = compressor.maxCompressedLength(stringBytes.length);
            final byte[] compressed = new byte[maxLength];
            final int length = compressor.compress(stringBytes, 0, stringBytes.length, compressed, 0, maxLength);

            os.writeInt(stringBytes.length);
            os.writeInt(length);
            os.write(compressed, 0, length);
         } else {
            os.writeUTF(value.getSomeString());
         }
      }

      public Value read(FastByteArrayInputStream is) throws Exception {
         int number = is.readInt();
         UUID someUUID = new UUID(is.readLong(), is.readLong());

         String someString;
         if (useLZ4) {
            final int uncompressedLength = is.readInt();
            final int compressedLength = is.readInt();
            final byte[] bytes = new byte[compressedLength];
            final byte[] uncompressed = new byte[uncompressedLength];

            is.readFully(bytes);
            uncompressor.uncompress(bytes, 0, uncompressed, 0, uncompressedLength);
            someString = new String(uncompressed, "UTF-8");
         } else {
            someString = is.readUTF();
         }

         return new Value(number, someString, someUUID);
      }
   }
}
