package io;

import com.hazelcast.nio.FastByteArrayInputStream;
import com.hazelcast.nio.FastByteArrayOutputStream;
import com.hazelcast.nio.TypeSerializer;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4Uncompressor;
import org.xerial.snappy.Snappy;

import java.util.UUID;

public class ValueSerializer implements TypeSerializer<Value> {
   private static LZ4Compressor compressor;
   private static LZ4Uncompressor uncompressor;
   private final Compression comp;

   public ValueSerializer(Compression comp) {
      this.comp = comp;
      if (comp == Compression.LZ4_FAST) {
         compressor = LZ4Factory.unsafeInstance().fastCompressor();
         uncompressor = LZ4Factory.unsafeInstance().uncompressor();
      } else if (comp == Compression.LZ4_HIGH) {
         compressor = LZ4Factory.unsafeInstance().highCompressor();
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

      if (compressor != null) {
         final byte[] stringBytes = value.getSomeString().getBytes("UTF-8");
         final int maxLength = compressor.maxCompressedLength(stringBytes.length);
         final byte[] compressed = new byte[maxLength];
         final int length = compressor.compress(stringBytes, 0, stringBytes.length, compressed, 0, maxLength);

         os.writeInt(stringBytes.length);
         os.writeInt(length);
         os.write(compressed, 0, length);
      } else if (comp == Compression.SNAPPY) {
         final byte[] compressed = Snappy.compress(value.getSomeString());
         os.writeInt(compressed.length);
         os.write(compressed);
      } else {
         os.writeUTF(value.getSomeString());
      }
   }

   public Value read(FastByteArrayInputStream is) throws Exception {
      int number = is.readInt();
      UUID someUUID = new UUID(is.readLong(), is.readLong());
      String someString;

      if (uncompressor != null) {
         final int uncompressedLength = is.readInt();
         final int compressedLength = is.readInt();
         final byte[] bytes = new byte[compressedLength];
         final byte[] uncompressed = new byte[uncompressedLength];

         is.readFully(bytes);
         uncompressor.uncompress(bytes, 0, uncompressed, 0, uncompressedLength);
         someString = new String(uncompressed, "UTF-8");
      } else if (comp == Compression.SNAPPY) {
         final int length = is.readInt();
         final byte[] bytes = new byte[length];

         is.readFully(bytes);
         final byte[] uncompressed = Snappy.uncompress(bytes);
         someString = new String(uncompressed, "UTF-8");
      } else {
         someString = is.readUTF();
      }

      return new Value(number, someString, someUUID);
   }
}
