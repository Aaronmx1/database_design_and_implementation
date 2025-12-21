package simpledb.log;

import java.util.Iterator;
import simpledb.server.SimpleDB;
import simpledb.file.Page;

public class LogTest {
   private static LogMgr lm;

   public static void main(String[] args) {
      SimpleDB db = new SimpleDB("logtest", 400, 8);
      lm = db.logMgr();

      printLogRecords("The initial empty log file:");  //print an empty log file
      System.out.println("done");
      createRecords(1, 35);
      printLogRecords("The log file now has these records:");
      createRecords(36, 70);
      lm.flush(65);
      printLogRecords("The log file now has these records:");
   }

   private static void printLogRecords(String msg) {
      System.out.println(msg);
      Iterator<byte[]> iter = lm.iterator();
      while (iter.hasNext()) {
         byte[] rec = iter.next();
         Page p = new Page(rec);               // AM: Page allocated to hold a single Log record's bytes.  Record wrapped by Page to utilize helper methods.
         String s = p.getString(0);    // AM: Gets the string at the start of that specific record
         int npos = Page.maxLength(s.length());// AM: Stores position where string ends
         int val = p.getInt(npos);             // AM: Retrieves integer based on position where string ends
         System.out.println("[" + s + ", " + val + "]");
      }
      System.out.println();
   }

   private static void createRecords(int start, int end) {
      System.out.print("Creating records: ");
      for (int i=start; i<=end; i++) {
         byte[] rec = createLogRecord("record"+i, i+100);
         int lsn = lm.append(rec);
         System.out.print(lsn + " ");
      }
      System.out.println();
   }

   // Create a log record having two values: a string and an integer.
   private static byte[] createLogRecord(String s, int n) {
      int spos = 0;                                // AM: Start of string
      int npos = spos + Page.maxLength(s.length());// AM: Size of the String field + String (Length of String + String)
      // AM: Formatted as [Len][String][Int]
      byte[] b = new byte[npos + Integer.BYTES];   // AM: Byte array containing [(integer byte for "s" + size of string for "s") + (integer byte for "n")]
      Page p = new Page(b);                        // AM: Wraps the Byte array in a Byte Buffer managed by the Page class
      p.setString(spos, s);
      p.setInt(npos, n);
      return b;
   }
}
