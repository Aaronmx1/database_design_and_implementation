package simpledb.tx.concurrency;

import java.util.*;
import simpledb.file.BlockId;

/** 
 * AM - Hybrid personal & AI write-up LockTable architecture 
 * The LockTable class manages the locking of database blocks for read/write access.
 * * ARCHITECTURE OVERVIEW:
 * This class implements "Fine-Grained Locking" using a two-level synchronization strategy
 * to maximize concurrency and performance.
 * * 1. THE GLOBAL MAP (The "Registry")
 * - We maintain a global HashMap: Map<BlockId, BlockLock> locks.
 * - Purpose: To quickly look up the specific lock object associated with a file block.
 * - Synchronization: We synchronize on the 'locks' map ONLY for the brief microsecond
 * required to .get() or .put() the BlockLock object. This prevents threads from
 * blocking each other at the map level unless they are modifying the map structure simultaneously.
 * * 2. THE LOCAL MONITOR (The "Waiting Room")
 * - Each block has its own dedicated 'BlockLock' object.
 * - Purpose: This object acts as a specific monitor for that single block.
 * - Synchronization: Threads synchronize on this specific 'BlockLock' instance to check
 * status (isXLocked, etc.) or wait().
 * - Benefit: If Thread A is waiting for Block 1, and Thread B is working on Block 99,
 * they do not block each other because they are synchronizing on different objects.
 * * 3. THE LOCKING LOGIC (S-Lock vs X-Lock)
 * - S-Lock (Shared): Used for Reading. 
 * - We check if the block is currently X-Locked (being written to).
 * - If yes, we wait on the BlockLock object.
 * - If no, we increment the reader count (val++) and proceed.
 * - X-Lock (Exclusive): Used for Writing.
 * - The Transaction Manager (ConcurrencyMgr) typically requests an S-Lock first
 * before upgrading to an X-Lock.
 * - We check if the block is currently S-Locked by others (val > 1) OR X-Locked (val < 0).
 * - If either is true, we wait. This ensures we don't overwrite data while others are reading.
 * * 4. NOTIFICATION STRATEGY ("Thundering Herd" Solution)
 * - When a transaction calls unlock(), we decrement the lock count.
 * - If the count reaches 0 (free), we call blkLock.notifyAll().
 * - Critical Optimization: This notifyAll() is called on the specific BlockLock object,
 * so it ONLY wakes up threads waiting for THAT specific block. Threads waiting for
 * other blocks remain asleep, saving significant CPU resources.
 */

/**
 * The lock table, which provides methods to lock and unlock blocks.
 * If a transaction requests a lock that causes a conflict with an
 * existing lock, then that transaction is placed on a wait list.
 * There is only one wait list for all blocks.
 * When the last lock on a block is unlocked, then all transactions
 * are removed from the wait list and rescheduled.
 * If one of those transactions discovers that the lock it is waiting for
 * is still locked, it will place itself back on the wait list.
 * @author Edward Sciore
 */
class LockTable {
   private static final long MAX_TIME = 10000; // 10 seconds
   
   /*
   // AM: Exercise 5.53 - Not added, only commented out
   private Map<BlockId,Integer> locks = new HashMap<BlockId,Integer>();
   */
  private Map<BlockId, BlockLock> locks = new HashMap<BlockId,BlockLock>();   // AM: Global Map (finding the block)

   /**
    * AM: A specific lock object for a specific block.
    *      Threads waiting for this block will wait on THIS object instance.
    *       Is a Local Monitor (waiting for the block)
    */
   class BlockLock {
      int val = 0;   // AM: The lock value: -1 (X-Lock), 0 (None), >0 (S-Lock)

      // AM: Helper: Is this block X-locked?
      boolean isXLocked(){
         return val < 0;
      }

      // AM: Helper: Is this block S-locked by more than 1 person?
      boolean isSLocked(){
         return val > 1;
      }
   }
   
   /**
    * Grant an SLock on the specified block.
    * If an XLock exists when the method is called,
    * then the calling thread will be placed on a wait list
    * until the lock is released.
    * If the thread remains on the wait list for a certain 
    * amount of time (currently 10 seconds),
    * then an exception is thrown.
    * @param blk a reference to the disk block
    */
  public void sLock(BlockId blk){
      BlockLock blkLock;

      // AM: Get or Create the specific lock object (Fast!)
      synchronized(locks){
         blkLock = locks.get(blk);  // AM: Will only block a thread to the HashMap table for a microsecond to process the .get()
         if(blkLock == null){
            blkLock = new BlockLock();
            locks.put(blk, blkLock);
         }
      }

      // AM: Synchronize ONLY on this specific block. Allows multiple blocks to perform concurrently when working on different blocks.
      synchronized(blkLock){
         try{
            long timestamp = System.currentTimeMillis();

            // AM: Wait only if X-Locked
            while(blkLock.isXLocked() && !waitingTooLong(timestamp)){
               blkLock.wait(MAX_TIME);    // AM: If block is locked, continue to wait
            }

            if(blkLock.isXLocked()){
               throw new LockAbortException();
            }
            
            blkLock.val++;          // AM: Grant S-Lock
         }
         catch(InterruptedException e){
            throw new LockAbortException();
         }
      }
  }
   
   /**
    * Grant an XLock on the specified block.
    * If a lock of any type exists when the method is called,
    * then the calling thread will be placed on a wait list
    * until the locks are released.
    * If the thread remains on the wait list for a certain 
    * amount of time (currently 10 seconds),
    * then an exception is thrown.
    * @param blk a reference to the disk block
    */
   // AM: Exercise 5.53
   public void xLock(BlockId blk){
      BlockLock blkLock;

      // AM: 1. Get or Create block lock
      synchronized(locks){          // AM: Synchronize on HashMap lock table to give active thread control over lock table during processing
         blkLock = locks.get(blk);
         if(blkLock == null){
            blkLock = new BlockLock();
            locks.put(blk, blkLock);
         }
      }

      // AM: 2. Synchronize ONLY on this specific block
      synchronized(blkLock){
         try{
            long timestamp = System.currentTimeMillis();

            // AM: Wait only if X-locked (ie. someone is writing)
            while((blkLock.isSLocked() || blkLock.isXLocked()) && !waitingTooLong(timestamp)){
               blkLock.wait(MAX_TIME); // AM: Waits on this block's specific queue
            }

            if(blkLock.isXLocked()){
               throw new LockAbortException();
            }

            blkLock.val = -1;    // AM: Grant X-Lock
         }
         catch(InterruptedException e){
            throw new LockAbortException();
         }
      }
   }
   
   /**
    * Release a lock on the specified block.
    * If this lock is the last lock on that block,
    * then the waiting transactions are notified.
    * @param blk a reference to the disk block
    */
   // AM: Exercise 5.53
   public void unlock(BlockId blk){
      BlockLock blkLock;

      // AM: 1. Retrieve the lock
      synchronized(locks){
         blkLock = locks.get(blk);
      }

      if(blkLock == null){    // AM: Should not happen if protocol is followed
         return;
      }

      // AM: 2. Sync on block to modify state and notify
      synchronized(blkLock){
         if(blkLock.val > 1){
            blkLock.val--;       // AM: Release lock on block
         }
         else{
            blkLock.val = 0;
            blkLock.notifyAll();    // AM: Only wakes threads waiting for THIS block
         }
      }
   }

   private boolean waitingTooLong(long starttime) {
      return System.currentTimeMillis() - starttime > MAX_TIME;
   }
}
