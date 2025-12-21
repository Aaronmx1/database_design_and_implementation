package simpledb.file;

import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.util.Date;     // AM: For Date objects

/**
 * AM: Cursor movement related to the ByteBuffer
 *       Absolute operation -> like setInt(offset, n) DON'T move the cursor and instead point to a specific point and start retrieving data
 *       Relative operation -> like getInt() and get(b) DO move the cursor and allows for seqeuentially reading data
 */

public class Page {
   private ByteBuffer bb;
   public static Charset CHARSET = StandardCharsets.US_ASCII;

   // For creating data buffers
   // AM: Used by the Buffer Manager and created inside the Buffer constructor. Page is the raw memory container.
   public Page(int blocksize) {
      bb = ByteBuffer.allocateDirect(blocksize);   // AM: Allocates memory to hold block unit retrieved from disk.
   }
   
   // For creating log pages
   // AM: Wraps a Byte array in a Byte Buffer that allows for direct manipulation of that Byte array
   public Page(byte[] b) {
      bb = ByteBuffer.wrap(b);   // AM: Creates a "view" or "window" directly over the Byte array in memory
   }

   public int getInt(int offset) {
      return bb.getInt(offset);
   }

   public void setInt(int offset, int n) {
      // AM: Check if the 4 bytes for an int will fit
      if(offset + Integer.BYTES > bb.capacity()){
         throw new RuntimeException("Page buffer exceeded for int at offset " + offset);
      }
      bb.putInt(offset, n);
   }

   public byte[] getBytes(int offset) {
      bb.position(offset);          // AM: Positions Byte Buffer at offset of integer length of string
      int length = bb.getInt();     // AM: Retrieves string length integer and advances Byte Buffer cursor by 4
      byte[] b = new byte[length];  // AM: Creates byte array to store string size
      bb.get(b);                    // AM: Cursor is now sitting at the start of string contained in Byte Buffer and stores these bytes inside buffer array (ie. "b")
      return b;
   }

   public void setBytes(int offset, byte[] b) {
      // AM: Check if the 4-byte length + the byte array will fit
      // AM: starting offset + integer length of string + string length
      if(offset + Integer.BYTES + b.length > bb.capacity()){
         throw new RuntimeException("Page buffer exceeded for byte array at offset " + offset);
      }
      bb.position(offset); // AM: Move cursor to the start
      bb.putInt(b.length); // AM: Write 4 bytes (cursor moves 4)
      bb.put(b);           // AM: Write string bytes (cursor moves n)
   }
   
   public String getString(int offset) {
      byte[] b = getBytes(offset);     // AM: Retrieves Bytes at specific offset
      return new String(b, CHARSET);   // AM: Translates raw bytes into human-readable text
   }

   public void setString(int offset, String s) {
      byte[] b = s.getBytes(CHARSET);
      setBytes(offset, b);
   }

   public static int maxLength(int strlen) {
      float bytesPerChar = CHARSET.newEncoder().maxBytesPerChar();
      return Integer.BYTES + (strlen * (int)bytesPerChar);
   }

   // AM: Handle Boolean inputs
   public void setBool(int offset, boolean b){
      if(offset + Byte.BYTES > bb.capacity()){
         throw new RuntimeException("Page buffer exceeded for boolean at offset " + offset);
      }

      // Convert boolean to a byte
      byte bool = (b ? (byte) 1: (byte) 0);

      // Put the byte into the buffer at the offset
      bb.put(offset, bool);
   }

   // AM: Retrieve Boolean
   public boolean getBool(int offset){
      // Read the byte from the offset
      byte bool = bb.get(offset);

      // Return true if that byte is 1, false otherwise
      return bool == 1;
   }

   // AM: Handle Date inputs
   public void setDate(int offset, Date date){
      if(offset + Long.BYTES > bb.capacity()){
         throw new RuntimeException("Page buffer exceeded for date at offset " + offset);
      }

      // Get the long value from the Date object
      long longVal = date.getTime();

      // Write the long to the buffer using the "absolute" putLong
      bb.putLong(offset, longVal);
   }

   // AM: Retrieve Date
   public Date getDate(int offset){
      // Read the long value from the buffer
      long longVal = bb.getLong(offset);

      // Create a new Date object from that long
      return new Date(longVal);
   }

   // a package private method, needed by FileMgr
   ByteBuffer contents() {
      bb.position(0);
      return bb;
   }
}
