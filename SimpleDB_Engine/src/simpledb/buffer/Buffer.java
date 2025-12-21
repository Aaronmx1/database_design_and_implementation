package simpledb.buffer;

import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.file.Page;
import simpledb.log.LogMgr;

/**
 * An individual buffer. A databuffer wraps a page 
 * and stores information about its status,
 * such as the associated disk block,
 * the number of times the buffer has been pinned,
 * whether its contents have been modified,
 * and if so, the id and lsn of the modifying transaction.
 * @author Edward Sciore
 */

/**
 * AM - Hybrid personal & AI write-up architecture 
 * The Buffer class is the "Unit of Management" for memory.
 * While a Page holds the raw data, a Buffer holds the *metadata* about that data.
 * * ARCHITECTURE OVERVIEW:
 * * 1. THE CONTAINER (Data + State)
 * - It wraps a raw 'Page' object (contents) but adds critical status info:
 * - Which disk block is this? (BlockId)
 * - Is it currently in use? (pins)
 * - Has it been modified? (txnum, lsn)
 * * 2. THE GATEKEEPER (Traffic Logic)
 * - It bridges the gap between the Memory layer and the Disk layer.
 * - assignToBlock(): Loads data from FileMgr into memory.
 * - flush(): Saves data from memory to FileMgr.
 * * 3. WAL ENFORCEMENT (Durability)
 * - Before flushing dirty data to disk, the Buffer checks the Log Sequence Number (LSN).
 * - It ensures that the LogMgr has saved the corresponding log record before the Page writes its data. This guarantees that we never have "orphaned" updates
 * on disk without a log history.
 */

public class Buffer {
   private FileMgr fm;
   private LogMgr lm;
   private Page contents;
   private BlockId blk = null;
   private int pins = 0;
   private int txnum = -1;    // AM: Identifies if a modification has been made to the Buffer's Page and maintains the transaction number
   private int lsn = -1;      // AM: Log Sequence Number holds the most recent log record when an update is made by a transaction.

   public Buffer(FileMgr fm, LogMgr lm) {
      this.fm = fm;
      this.lm = lm;
      contents = new Page(fm.blockSize());   // AM: Points to a specific memory address on the heap (eg. Memory Address 0x1234)
   }
   
   public Page contents() {
      return contents;
   }

   /**
    * Returns a reference to the disk block
    * allocated to the buffer.
    * @return a reference to a disk block
    */
   public BlockId block() {
      return blk;
   }

   public void setModified(int txnum, int lsn) {
      this.txnum = txnum;
      if (lsn >= 0)
         this.lsn = lsn;
   }

   /**
    * Return true if the buffer is currently pinned
    * (that is, if it has a nonzero pin count).
    * @return true if the buffer is pinned
    */
   public boolean isPinned() {
      return pins > 0;
   }
   
   public int modifyingTx() {
      return txnum;
   }

   /**
    * Reads the contents of the specified block into
    * the contents of the buffer.
    * If the buffer was dirty, then its previous contents
    * are first written to disk.
    * @param b a reference to the data block
    */
   void assignToBlock(BlockId b) {
      flush();                // AM: Flush Buffer to Disk
      blk = b;
      fm.read(blk, contents); // AM: Triggers FileMgr to retrieve contents from disk and stores them into the Page object ('contents') owned by this Buffer.
      pins = 0;
   }
   
   /**
    * Write the buffer to its disk block if it is dirty.
    */
   void flush() {
      if (txnum >= 0) {
         lm.flush(lsn);             // AM: LogMgr flushes Log to Disk
         fm.write(blk, contents);   // AM: Writes Buffer to Disk
         txnum = -1;
      }
   }

   /**
    * Increase the buffer's pin count.
    */
   void pin() {
      pins++;
   }

   /**
    * Decrease the buffer's pin count.
    */
   void unpin() {
      pins--;
   }
}