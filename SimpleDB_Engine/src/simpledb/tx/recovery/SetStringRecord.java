package simpledb.tx.recovery;

import simpledb.file.BlockId;
import simpledb.file.Page;
import simpledb.log.LogMgr;
import simpledb.tx.Transaction;

public class SetStringRecord implements LogRecord {
   private int txnum, offset;
   private String val;
   private BlockId blk;

   /**
    * Create a new setint log record.
    * @param bb the bytebuffer containing the log values
    */
   public SetStringRecord(Page p) {
      int tpos = Integer.BYTES;
      txnum = p.getInt(tpos);
      int fpos = tpos + Integer.BYTES;
      String filename = p.getString(fpos);
      int bpos = fpos + Page.maxLength(filename.length());
      int blknum = p.getInt(bpos);
      blk = new BlockId(filename, blknum);
      int opos = bpos + Integer.BYTES;
      offset = p.getInt(opos);
      int vpos = opos + Integer.BYTES;      
      val = p.getString(vpos);
   }

   public int op() {
      return SETSTRING;
   }

   public int txNumber() {
      return txnum;
   }

   public String toString() {
      return "<SETSTRING " + txnum + " " + blk + " " + offset + " " + val + ">";
   }

   /**
    * Replace the specified data value with the value saved in the log record.
    * The method pins a buffer to the specified block,
    * calls setInt to restore the saved value,
    * and unpins the buffer.
    * @see simpledb.tx.recovery.LogRecord#undo(int)
    */
   public void undo(Transaction tx) {
      tx.pin(blk);
      tx.setString(blk, offset, val, false); // don't log the undo!
      tx.unpin(blk);
   }

   /**
    * A static method to write a setInt record to the log.
    * This log record contains the SETINT operator,
    * followed by the transaction id, the filename, number,
    * and offset of the modified block, and the previous
    * integer value at that offset.
    * @return the LSN of the last log value
    */
   /**
    * AM: Mental image of the byte array record you are building
    *    [ Op Code (4) ] [ TxNum (4) ] [ Filename (Len+Chars) ] [ BlkNum (4) ] [ Block Offset (4) ] [ Old String Value (Len+Chars) ]
         ^               ^             ^                        ^              ^                  ^
         0               tpos          fpos                     bpos           opos               vpos
    */
   public static int writeToLog(LogMgr lm, int txnum, BlockId blk, int offset, String val) {
      int tpos = Integer.BYTES;
      int fpos = tpos + Integer.BYTES;
      int bpos = fpos + Page.maxLength(blk.fileName().length());
      int opos = bpos + Integer.BYTES;
      int vpos = opos + Integer.BYTES;
      int reclen = vpos + Page.maxLength(val.length());
      byte[] rec = new byte[reclen];
      Page p = new Page(rec);             // AM: Wrapper to write contents of record in byte array
      p.setInt(0, SETSTRING);      // AM: Transaction type. The first integer (offset 0) is always the Operator ID (e.g., SETSTRING = 5). This tells the Recovery Manager how to parse the rest of the bytes.
      p.setInt(tpos, txnum);              // AM: Transaction number. Used during recovery to identify which transaction this update belongs to.
      p.setString(fpos, blk.fileName());  // AM: Transaction filename
      p.setInt(bpos, blk.number());       // AM: Transaction Block ID.  The log needs to know exactly which block on the disk was modified.
      p.setInt(opos, offset);             // AM: The offset inside the data block where the value was changed. Example: If you updated a student's name at byte 40 of Block 100, this offset value is 40.
      p.setString(vpos, val);             // AM: Where to write the Old String record. Is the position in the log record byte array where we write the actual string value. Note that this is the OLD value (undo information), not the new value!
      return lm.append(rec);              // AM: Appends Log record to end of Log on disk. Sends the populated byte array to the LogMgr.
   }
}