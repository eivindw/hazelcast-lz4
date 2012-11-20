package io;

import java.io.Serializable;
import java.util.UUID;

public class Value implements Serializable {
   private int number;
   private String someString;
   private UUID someUUID;

   public Value(int number, String someString, UUID someUUID) {
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

