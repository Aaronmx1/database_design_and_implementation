package simpledb.tx;

import java.util.*;
import simpledb.file.BlockId;
import simpledb.buffer.*;

/**
 * Manage the transaction's currently-pinned buffers. 
 * @author Edward Sciore
 */

/**
 * AM - Hybrid personal & AI write-up architecture 
 * The BufferList class is the "Bookkeeper" for a single Transaction.
 * It tracks which buffers are currently pinned by *this specific* transaction.
 * * ARCHITECTURE OVERVIEW:
 * * 1. THE PRIVATE TRACKER (The "Backpack")
 * - The global BufferMgr manages the pool, but it doesn't know which transaction
 * "owns" which pin.
 * - BufferList maintains a private Map<BlockId, Buffer> for the transaction.
 * * 2. AUTOMATIC CLEANUP (Resource Management)
 * - When a Transaction commits or rolls back, it must release ALL its pins.
 * - Instead of the programmer manually calling unpin() for every single block read,
 * the Transaction simply calls unpinAll().
 * - BufferList iterates through its private list and releases everything back 
 * to the global BufferMgr.
 * * 3. CACHE OPTIMIZATION
 * - If a transaction asks for Block 5 twice, BufferList sees it's already in the
 * private map and returns it immediately, avoiding a redundant call to the global BufferMgr.
 */
class BufferList {
   private Map<BlockId,Buffer> buffers = new HashMap<>();  // AM: Tracks Buffers in use by a Transaction
   private List<BlockId> pins = new ArrayList<>();         // AM: Tracks list of pinned Blocks
   private BufferMgr bm;
  
   public BufferList(BufferMgr bm) {
      this.bm = bm;
   }
   
   /**
    * Return the buffer pinned to the specified block.
    * The method returns null if the transaction has not
    * pinned the block.
    * @param blk a reference to the disk block
    * @return the buffer pinned to that block
    */
   Buffer getBuffer(BlockId blk) {
      return buffers.get(blk);
   }
   
   /**
    * Pin the block and keep track of the buffer internally.
    * @param blk a reference to the disk block
    */
   void pin(BlockId blk) {
      Buffer buff = bm.pin(blk);
      buffers.put(blk, buff);    // AM: Store txn Buffer in HashMap
      pins.add(blk);
   }
   
   /**
    * Unpin the specified block.
    * @param blk a reference to the disk block
    */
   void unpin(BlockId blk) {
      Buffer buff = buffers.get(blk);
      bm.unpin(buff);
      pins.remove(blk);
      if (!pins.contains(blk))
         buffers.remove(blk);
   }
   
   /**
    * Unpin any buffers still pinned by this transaction.
    */
   void unpinAll() {
      for (BlockId blk : pins) {
         Buffer buff = buffers.get(blk);
         bm.unpin(buff);
      }
      buffers.clear();
      pins.clear();
   }
}