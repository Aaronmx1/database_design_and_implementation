package simpledb.log;

import java.util.Iterator;
import simpledb.file.*;
import simpledb.buffer.*;     // AM: Imported to utilize Buffer instead of generating local Page inside constructor

/**
 * A class that provides the ability to move through the
 * records of the log file in reverse order.
 * 
 * @author Edward Sciore
 */

/**
 * AM: LogIterator reads exactly from the physical disk file. The log must be flushed prior to being read in.
 */
class LogIterator implements Iterator<byte[]> {
   private FileMgr fm;
   private BlockId blk;
   private Page p;
   private int currentpos;
   private int boundary;

   private BufferMgr bm;      // AM: Added to utilize Buffer instead of local Page for Log
   private Buffer logBuffer;  // AM: Maintains Log Buffer Page inside BufferMgr

   /**
    * Creates an iterator for the records in the log file,
    * positioned after the last log record.
    */
   /* AM: 4.12 Exercise - Commented out to implement new version
   public LogIterator(FileMgr fm, BlockId blk) {
      this.fm = fm;
      this.blk = blk;
      byte[] b = new byte[fm.blockSize()];
      p = new Page(b);
      moveToBlock(blk);
   }
   */
   public LogIterator(FileMgr fm, BlockId blk, BufferMgr bm){
      this.fm = fm;
      this.blk = blk;
      this.bm = bm;
      moveToBlock(blk);    // AM: Performs pin/unpin logic and passes Log content to Buffer
   }

   /**
    * Determines if the current log record
    * is the earliest record in the log file.
    * @return true if there is an earlier record
    */
   /**
    *  AM: Handles transition between blocks
    *    currentpos<fm.blockSize() <- We haven't reached the "top" (end) of the current block yet. (We read right-to-left, or bottom-to-top)
    *    currentpos == 400 <- Means we are at the top of the block
    *    blk.number() == 0  <- Means we are at Block 0
    */
   /* AM: 4.12 Exercise - Commented out to implement new version
   public boolean hasNext() {
      return currentpos<fm.blockSize() || blk.number()>0;
   }
   */
   public boolean hasNext(){
      // AM: Logic to check if more records exist
      boolean hasMore = currentpos < fm.blockSize() || blk.number() > 0;

      // AM: If no more records exist, unpin the final Buffer
      if(!hasMore && logBuffer != null){
         bm.unpin(logBuffer);
         logBuffer = null;    // AM: Prevent double unpinning
      }

      return hasMore;
   }

   /**
    * Moves to the next log record in the block.
    * If there are no more log records in the block,
    * then move to the previous block
    * and return the log record from there.
    * @return the next earliest log record
    */
   public byte[] next() {
      if (currentpos == fm.blockSize()) {
         blk = new BlockId(blk.fileName(), blk.number()-1);
         moveToBlock(blk);
      }
      byte[] rec = p.getBytes(currentpos);      // AM: Returns Byte Array containing record
      currentpos += Integer.BYTES + rec.length; // AM: Move to position to next record by accounting for integer representation of record size and the records length
      return rec;
   }

   /**
    * Moves to the specified log block
    * and positions it at the first record in that block
    * (i.e., the most recent one).
    */
   /* AM: 4.12 Exercise - Commented out to implement new version
   private void moveToBlock(BlockId blk) {
      fm.read(blk, p);                       // AM: Reads Log Block from File into Page
      boundary = p.getInt(0);        // AM: Retrieves pointer to last record written to this Block
      currentpos = boundary;                // AM: Moves iterator to end of the valid data so it can work backwards
   }
    */
   private void moveToBlock(BlockId blk){
      // AM: 1. Unpin the previous buffer (if any)
      if(logBuffer != null){
         bm.unpin(logBuffer);
      }

      // AM: 2. Pin the new block
      logBuffer = bm.pin(blk);

      // AM: 3. Get the Page reference
      this.p = logBuffer.contents();

      // AM: 4. Set pointers
      boundary = p.getInt(0);
      currentpos = boundary;
   }
}
