package simpledb.tx;

import java.util.*;
import simpledb.file.BlockId;
import simpledb.buffer.*;

/**
 * Manage the transaction's currently-pinned buffers. 
 * @author Edward Sciore
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