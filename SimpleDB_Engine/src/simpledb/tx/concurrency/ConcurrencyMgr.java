package simpledb.tx.concurrency;

import java.util.HashMap;
import java.util.Map;

import simpledb.file.BlockId;

/**
 * The concurrency manager for the transaction.
 * Each transaction has its own concurrency manager. 
 * The concurrency manager keeps track of which locks the 
 * transaction currently has, and interacts with the
 * global lock table as needed. 
 * @author Edward Sciore
 */
/**
 * AM - Hybrid personal & AI write-up architecture
 * The ConcurrencyMgr class handles the Isolation property of ACID.
 * Each Transaction has its own unique ConcurrencyMgr instance.
 * * ARCHITECTURE OVERVIEW:
 * * 1. THE LOCAL CACHE (Performance Optimization)
 * - It maintains a local HashMap (Map<BlockId, String> locks) of locks held ONLY
 * by this specific transaction.
 * - Benefit: If the transaction already owns a lock, it doesn't need to ask the
 * global LockTable again. This reduces contention on the global lock table.
 * * 2. THE PROTOCOL ENFORCER (Strict 2PL)
 * - It enforces the Strict Two-Phase Locking protocol.
 * - Phase 1 (Growing): It acquires locks as needed (via sLock/xLock).
 * - Phase 2 (Shrinking): It NEVER releases locks individually. It only releases
 * ALL locks at once when release() is called (during Commit/Rollback).
 * * 3. THE GLOBAL LIAISON
 * - It acts as the middle-man between the Transaction and the global LockTable.
 * - It handles the logic of "Lock Upgrades" (Requesting an S-Lock first,
 * then upgrading to X-Lock if needed).
 */
public class ConcurrencyMgr {

   /**
    * The global lock table. This variable is static because 
    * all transactions share the same table.
    */
   private static LockTable locktbl = new LockTable();       // AM: Global lock table for the whole database
   private Map<BlockId,String> locks  = new HashMap<BlockId,String>();  // AM: Local lock mapping to avoid having to check LockTbl if not necessary

   /**
    * Obtain an SLock on the block, if necessary.
    * The method will ask the lock table for an SLock
    * if the transaction currently has no locks on that block.
    * @param blk a reference to the disk block
    */
   public void sLock(BlockId blk) {
      if (locks.get(blk) == null) {
         locktbl.sLock(blk);
         locks.put(blk, "S");
      }
   }

   /**
    * Obtain an XLock on the block, if necessary.
    * If the transaction does not have an XLock on that block,
    * then the method first gets an SLock on that block
    * (if necessary), and then upgrades it to an XLock.
    * @param blk a reference to the disk block
    */
   public void xLock(BlockId blk) {
      if (!hasXLock(blk)) {
         sLock(blk);                // AM: <- Step 1: Ensure have at least an S-Lock
         locktbl.xLock(blk);        // AM: <- Step 2: Try to upgrade to X-Lock
         locks.put(blk, "X");       // AM: <- Step 3: Update local status
      }
   }

   /**
    * Release all locks by asking the lock table to
    * unlock each one.
    */
   public void release() {
      for (BlockId blk : locks.keySet()) 
         locktbl.unlock(blk);        // AM: Unlocks blks associated to Transactions
      locks.clear();
   }

   private boolean hasXLock(BlockId blk) {
      String locktype = locks.get(blk);
      return locktype != null && locktype.equals("X");
   }
}
