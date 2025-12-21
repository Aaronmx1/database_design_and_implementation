package simpledb.buffer;

import simpledb.file.*;
import simpledb.log.LogMgr;
import java.util.*; // AM: imports the Map algorithm

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * 
 * @author Edward Sciore
 *
 */
/**
 * AM - Hybrid personal & AI write-up architecture
 * The BufferMgr class manages the "Buffer Pool," a fixed-size cache in RAM.
 * Its goal is to minimize expensive Disk I/O by keeping frequently used blocks in memory.
 * * ARCHITECTURE OVERVIEW:
 * * 1. THE POOL (Finite Resources)
 * - We have a limited array of 'Buffer' objects (e.g., 8 buffers).
 * - If 9 transactions want different blocks, someone has to wait or someone gets evicted.
 * * 2. PINNING STRATEGY (Traffic Control)
 * - pin(): "I need this block. Keep it in RAM."
 * - unpin(): "I am done. You can use this slot for someone else."
 * - If no buffers are available, pin() makes the calling thread WAIT().
 * * 3. REPLACEMENT POLICY (The Clock Algorithm)
 * - When we need to load a new block but the pool is full, we must choose a victim to evict.
 * - We implemented the "Clock Algorithm" (Exercise 4.11).
 * - It iterates through the pool in a circle, looking for an unpinned buffer to steal.
 * * 4. WRITE-AHEAD LOGGING SUPPORT
 * - Before a dirty buffer is written back to disk, BufferMgr checks with LogMgr.
 * - It ensures the relevant Log Record is saved to disk FIRST. This guarantees
 * we never have data on disk without a history of how it got there.
 */
public class BufferMgr {
   private Buffer[] bufferpool;
   private int numAvailable;
   private static final long MAX_TIME = 10000; // 10 seconds

   private int clockHand = 0; // AM: Maintains Buffer pool clock hand; starting at index 0
   private Map<BlockId, Buffer> bufferMap; // AM: Maintains HashMap using BlockId as the key to speed up search time to O(1)

   /**
    * Creates a buffer manager having the specified number
    * of buffer slots.
    * This constructor depends on a {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} object.
    * 
    * @param numbuffs the number of buffer slots to allocate
    */
   /*
    * AM: Exercise 4.11 - Not added, only commented out
    * public BufferMgr(FileMgr fm, LogMgr lm, int numbuffs) {
    * bufferpool = new Buffer[numbuffs];
    * numAvailable = numbuffs;
    * for (int i=0; i<numbuffs; i++)
    * bufferpool[i] = new Buffer(fm, lm);
    * }
    */
   public BufferMgr(FileMgr fm, LogMgr lm, int numbuffs) {
      bufferpool = new Buffer[numbuffs];
      numAvailable = numbuffs;
      bufferMap = new HashMap<>(); // AM: Initailize the Map
      for (int i = 0; i < numbuffs; i++) {
         bufferpool[i] = new Buffer(fm, lm);
      }
   }

   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * 
    * @return the number of available buffers
    */
   public synchronized int available() {
      return numAvailable;
   }

   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * 
    * @param txnum the transaction's id number
    */
   public synchronized void flushAll(int txnum) {
      for (Buffer buff : bufferpool)
         if (buff.modifyingTx() == txnum)
            buff.flush();
   }

   /**
    * Unpins the specified data buffer. If its pin count
    * goes to zero, then notify any waiting threads.
    * 
    * @param buff the buffer to be unpinned
    */
   public synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned()) {    // AM: Checks if any remaining pins exist on Buffer
         numAvailable++;
         notifyAll();
      }
   }

   /**
    * Pins a buffer to the specified block, potentially
    * waiting until a buffer becomes available.
    * If no buffer becomes available within a fixed
    * time period, then a {@link BufferAbortException} is thrown.
    * 
    * @param blk a reference to a disk block
    * @return the buffer pinned to that block
    */
   public synchronized Buffer pin(BlockId blk) {
      try {
         long timestamp = System.currentTimeMillis();
         Buffer buff = tryToPin(blk);
         while (buff == null && !waitingTooLong(timestamp)) {
            wait(MAX_TIME);
            buff = tryToPin(blk);
         }
         if (buff == null)
            throw new BufferAbortException();
         return buff;
      } catch (InterruptedException e) {
         throw new BufferAbortException();
      }
   }

   private boolean waitingTooLong(long starttime) {
      return System.currentTimeMillis() - starttime > MAX_TIME;
   }

   /**
    * Tries to pin a buffer to the specified block.
    * If there is already a buffer assigned to that block
    * then that buffer is used;
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * 
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   /*
    * AM: Exercise 4.11 - Not added, only commented out
    * private Buffer tryToPin(BlockId blk) {
    * Buffer buff = findExistingBuffer(blk);
    * if (buff == null) {
    * buff = chooseUnpinnedBuffer();
    * if (buff == null)
    * return null;
    * buff.assignToBlock(blk);
    * }
    * if (!buff.isPinned())
    * numAvailable--;
    * buff.pin();
    * return buff;
    * }
    */
   private Buffer tryToPin(BlockId blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null) // AM: Checks if an unpinned Buffer was found
            return null;

         // AM: If this buffer was holding an old block, remove that mapping
         BlockId oldBlk = buff.block();
         if (oldBlk != null) {
            bufferMap.remove(oldBlk);
         }

         buff.assignToBlock(blk); // AM: Assign new Block to Buffer

         // Add the new mapping
         bufferMap.put(blk, buff);
      }

      // AM: Reduce Buffers available if Buffer is not pinned
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();

      return buff;
   }

   /*
    * AM: Exercise 4.11 - Not added, only commented out
    * private Buffer findExistingBuffer(BlockId blk) {
    * for (Buffer buff : bufferpool) {
    * BlockId b = buff.block();
    * if (b != null && b.equals(blk))
    * return buff;
    * }
    * return null;
    * }
    */
   private Buffer findExistingBuffer(BlockId blk) {
      // AM: O(1) lookup instead of O(N) lookup
      return bufferMap.get(blk); // AM: Returns null if no Buffer matching blk found
   }

   /*
    * AM: Exercise 4.11 - Not added, only commented out
    * private Buffer chooseUnpinnedBuffer() {
    * for (Buffer buff : bufferpool)
    * if (!buff.isPinned())
    * return buff;
    * return null;
    * }
    */
   private Buffer chooseUnpinnedBuffer() {
      // AM: 1. Check every Buffer once (up to numbuffs times)
      for (int i = 0; i < bufferpool.length; i++) {

         // AM: 2. Calculate the index in the "Circle"
         // AM: If clockHand is 6 and i is 1, index is 7. Use Modulo to create circle.
         int index = (clockHand + i) % bufferpool.length;

         Buffer buff = bufferpool[index];

         // AM: 3. Pick the first unpinned Buffer we find
         if (!buff.isPinned()) {
            // AM: 4. Update the clockHand to the NEXT spot for next time
            clockHand = (index + 1) % bufferpool.length;
            return buff;
         }
      }

      // AM: If we looked at all Buffers and found nothing
      return null;
   }

}
