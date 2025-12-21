package simpledb.tx;

import simpledb.file.*;
import simpledb.log.LogMgr;
import simpledb.buffer.*;
import simpledb.tx.recovery.*;
import simpledb.tx.concurrency.ConcurrencyMgr;

import java.util.*;     // AM: Loads in HashMap class

/**
 * AM - Hybrid personal & AI write-up LockTable architecture 
 * The Transaction class is the "Hub" that manages the lifecycle of a user's interaction
 * with the database. It coordinates the ACID properties by delegating tasks to specific
 * sub-managers.
 * * * ARCHITECTURE OVERVIEW:
 * A "Transaction" is a logical group of instructions (reads/writes) that must succeed
 * or fail as a single unit. This class ensures that unit integrity.
 * * * 1. THE COORDINATOR (The "Hub")
 * - The Transaction object holds references to the global managers (FileMgr, LogMgr, BufferMgr)
 * and creates its own private helpers (RecoveryMgr, ConcurrencyMgr).
 * - It acts as the single point of entry for all upper-level database operations.
 * * * 2. LIFECYCLE MANAGEMENT
 * - START: Assigns a unique, persistent Transaction ID (TxID).
 * - RUNNING: 
 * - Requests specific blocks via pin().
 * - Reads/Writes data via getInt()/setInt().
 * - Automatically handles S-Locks and X-Locks (via ConcurrencyMgr) to prevent conflicts.
 * - TERMINATION (Commit/Rollback): 
 * - Ensures the Log is flushed to disk (Write-Ahead Logging).
 * - Releases all locks (Unblocking other threads).
 * - Unpins all buffers (Freeing memory).
 * * * 3. ACID ENFORCEMENT
 * - Atomicity & Durability: Delegated to RecoveryMgr.
 * - Ensures that on Commit, data is permanently saved.
 * - Ensures that on Rollback, all changes are undone.
 * - Isolation: Delegated to ConcurrencyMgr.
 * - Ensures that this transaction's changes are invisible to others until Commit.
 * - Consistency: Enforced by the implementation of the higher-level application logic,
 * supported by the reliability of this class.
 * * * 4. PERSISTENT IDENTITY (Exercise 5.50)
 * - Uses a sequence file ("txn_seq") to generate unique ID numbers that survive
 * system crashes and restarts, ensuring auditability and recovery integrity.
 */

/**
 * Provide transaction management for clients,
 * ensuring that all transactions are serializable, recoverable,
 * and in general satisfy the ACID properties.
 * @author Edward Sciore
 */
public class Transaction {
   //private static int nextTxNum = 0;
   private static final int END_OF_FILE = -1;   // AM: Creates a logical lock for a BlockId
   private RecoveryMgr    recoveryMgr;
   private ConcurrencyMgr concurMgr;
   private BufferMgr bm;
   private FileMgr fm;
   private int txnum;
   private BufferList mybuffers;

   // AM: Tracks the original size of files we appended to.
   //       Key = Filename, Value = Original Block Count (before we grew it)
   private Map<String, Integer> myAppends = new HashMap<>();
   
   /**
    * Create a new transaction and its associated 
    * recovery and concurrency managers.
    * This constructor depends on the file, log, and buffer
    * managers that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until either
    * {@link simpledb.server.SimpleDB#init(String)} or 
    * {@link simpledb.server.SimpleDB#initFileLogAndBufferMgr(String)} or
    * is called first.
    */
   public Transaction(FileMgr fm, LogMgr lm, BufferMgr bm) {
      this.fm = fm;
      this.bm = bm;
      //txnum       = nextTxNumber();     // AM: Exercise 5.50 - Not added, only commented out
      txnum       = nextTxNumber(fm);     // AM: Exercise 5.50 - Writes transaction number to log to preserve number sequence
      recoveryMgr = new RecoveryMgr(this, txnum, lm, bm);
      concurMgr   = new ConcurrencyMgr();
      mybuffers = new BufferList(bm);
   }
   
   /**
    * Commit the current transaction.
    * Flush all modified buffers (and their log records),
    * write and flush a commit record to the log,
    * release all locks, and unpin any pinned buffers.
    */
   public void commit() {
      recoveryMgr.commit();
      System.out.println("transaction " + txnum + " committed");
      concurMgr.release();    // AM: Releases locks from LockTbl
      mybuffers.unpinAll();   // AM: Unpins all Buffers related to Transaction
   }
   
   /**
    * Rollback the current transaction.
    * Undo any modified values,
    * flush those buffers,
    * write and flush a rollback record to the log,
    * release all locks, and unpin any pinned buffers.
    */
   /*     AM: Exercise 5.48 - Not added, only commented out
   public void rollback() {
      recoveryMgr.rollback();
      System.out.println("transaction " + txnum + " rolled back");
      concurMgr.release();
      mybuffers.unpinAll();
   }
   */
   public void rollback(){
      recoveryMgr.rollback();
      System.out.println("transaction " + txnum + " rolled back");

      // AM: Truncate files back to original size
      for(String filename : myAppends.keySet()){
         int originalSize = myAppends.get(filename);     // AM: Retrieves original size of file
         fm.truncate(filename, originalSize);
         System.out.println("Truncated " + filename + " back to " + originalSize + " blocks.");
      }

      concurMgr.release();                               // AM: Release locks from global Lock Table
      mybuffers.unpinAll();                              // AM: Unpin transactions buffers
   }
   
   /**
    * Flush all modified buffers.
    * Then go through the log, rolling back all
    * uncommitted transactions.  Finally, 
    * write a quiescent checkpoint record to the log.
    * This method is called during system startup,
    * before user transactions begin.
    */
   public void recover() {
      bm.flushAll(txnum);
      recoveryMgr.recover();
   }
   
   /**
    * Pin the specified block.
    * The transaction manages the buffer for the client.
    * @param blk a reference to the disk block
    */
   /* AM: Exercise 4.11 - Not added, only commented out
   public void pin(BlockId blk) {
      mybuffers.pin(blk);
   }
   */
  public void pin(BlockId blk){
      concurMgr.sLock(blk);      // AM: Added to lock block when block is pinned   
      mybuffers.pin(blk);
  }
   
   /**
    * Unpin the specified block.
    * The transaction looks up the buffer pinned to this block,
    * and unpins it.
    * @param blk a reference to the disk block
    */
   public void unpin(BlockId blk) {
      mybuffers.unpin(blk);
   }
   
   /**
    * Return the integer value stored at the
    * specified offset of the specified block.
    * The method first obtains an SLock on the block,
    * then it calls the buffer to retrieve the value.
    * @param blk a reference to a disk block
    * @param offset the byte offset within the block
    * @return the integer stored at that offset
    */
   /* AM: Exercise 4.11 - Not added, only commented out
   public int getInt(BlockId blk, int offset) {
      concurMgr.sLock(blk);
      Buffer buff = mybuffers.getBuffer(blk);
      return buff.contents().getInt(offset);
   }
   */
  // AM: Moved S-Lock process to when block is pinned.
  public int getInt(BlockId blk, int offset){
      Buffer buff = mybuffers.getBuffer(blk);
      return buff.contents().getInt(offset);
   }
   
   /**
    * Return the string value stored at the
    * specified offset of the specified block.
    * The method first obtains an SLock on the block,
    * then it calls the buffer to retrieve the value.
    * @param blk a reference to a disk block
    * @param offset the byte offset within the block
    * @return the string stored at that offset
    */
   /* AM: Exercise 4.11 - Not added, only commented out
   public String getString(BlockId blk, int offset) {
      concurMgr.sLock(blk);
      Buffer buff = mybuffers.getBuffer(blk);
      return buff.contents().getString(offset);
   }
   */
  // AM: Moved S-Lock process to when block is pinned.
  public String getString(BlockId blk, int offset){
      Buffer buff = mybuffers.getBuffer(blk);
      return buff.contents().getString(offset);
   }
   
   /**
    * Store an integer at the specified offset 
    * of the specified block.
    * The method first obtains an XLock on the block.
    * It then reads the current value at that offset,
    * puts it into an update log record, and 
    * writes that record to the log.
    * Finally, it calls the buffer to store the value,
    * passing in the LSN of the log record and the transaction's id. 
    * @param blk a reference to the disk block
    * @param offset a byte offset within that block
    * @param val the value to be stored
    */
   public void setInt(BlockId blk, int offset, int val, boolean okToLog) {
      concurMgr.xLock(blk);
      Buffer buff = mybuffers.getBuffer(blk);         // AM: Retrieves pinned Buffer from Transaction's private list
      int lsn = -1;
      if (okToLog)
         lsn = recoveryMgr.setInt(buff, offset, val); // AM: Write Transaction to Log before updating Buffer.
      Page p = buff.contents();                       // AM: Return reference to File being processed.
      p.setInt(offset, val);
      buff.setModified(txnum, lsn);                   // AM: Update Buffer to reflect Log Sequence Number update
   }
   
   /**
    * Store a string at the specified offset 
    * of the specified block.
    * The method first obtains an XLock on the block.
    * It then reads the current value at that offset,
    * puts it into an update log record, and 
    * writes that record to the log.
    * Finally, it calls the buffer to store the value,
    * passing in the LSN of the log record and the transaction's id. 
    * @param blk a reference to the disk block
    * @param offset a byte offset within that block
    * @param val the value to be stored
    */
   public void setString(BlockId blk, int offset, String val, boolean okToLog) {
      concurMgr.xLock(blk);
      Buffer buff = mybuffers.getBuffer(blk);
      int lsn = -1;
      if (okToLog)
         lsn = recoveryMgr.setString(buff, offset, val);
      Page p = buff.contents();
      p.setString(offset, val);
      buff.setModified(txnum, lsn);
   }

   /**
    * Return the number of blocks in the specified file.
    * This method first obtains an SLock on the 
    * "end of the file", before asking the file manager
    * to return the file size.
    * @param filename the name of the file
    * @return the number of blocks in the file
    */
   public int size(String filename) {
      BlockId dummyblk = new BlockId(filename, END_OF_FILE);
      concurMgr.sLock(dummyblk);       // AM: Obtains shared lock on "End of File". Effectively placing a "Do Not Disturb" sign on the file's length.
      return fm.length(filename);
   }
   
   /**
    * Append a new block to the end of the specified file
    * and returns a reference to it.
    * This method first obtains an XLock on the
    * "end of the file", before performing the append.
    * @param filename the name of the file
    * @return a reference to the newly-created disk block
    */
   /*    AM: Exercise 5.48 - Not added, only commented out
   public BlockId append(String filename) {
      BlockId dummyblk = new BlockId(filename, END_OF_FILE);
      concurMgr.xLock(dummyblk);       // AM: Obtains exclusive lock on "End of File"
      return fm.append(filename);
   }
   */
   public BlockId append(String filename){
      BlockId dummyblk = new BlockId(filename, END_OF_FILE);
      concurMgr.xLock(dummyblk);

      // AM: If this is the first time we are appending to this file.
      //       Remember how big it was originally.
      if(!myAppends.containsKey(filename)){
         myAppends.put(filename, fm.length(filename));   // AM: Stores filename and corresponding block number of files size before append is performed.
      }

      return fm.append(filename);
   }
   
   public int blockSize() {
      return fm.blockSize();
   }
   
   public int availableBuffs() {
      return bm.available();
   }

   /* 
   // AM: Exercise 5.50 - Not added, only commented out
   private static synchronized int nextTxNumber() {
      nextTxNum++;
      return nextTxNum;
   }
   */
   private static synchronized int nextTxNumber(FileMgr fm){
      // AM: 1. Prepare the specific block where we store the sequence
      BlockId blk = new BlockId("txn_seq", 0);

      // AM: 2. Read the current value from disk
      //    (Note: In a real system, we would cache this to avoid I/O on every Tx)
      int nextTxNum = 0;
      try{
         Page p = new Page(fm.blockSize());
         fm.read(blk, p);
         nextTxNum = p.getInt(0);   // Read value at offset 0
      }
      catch(Exception e){
         // AM: File might not exist yet (first run), start at 0
         nextTxNum = 0;
      }

      // AM: 3. Increment the counter
      nextTxNum++;

      // AM: 4. Write the NEW value back to disk immediately
      try{
         Page p = new Page(fm.blockSize());
         p.setInt(0, nextTxNum);
         fm.write(blk, p);
      }
      catch(Exception e){
         throw new RuntimeException("Could not update transaction sequence!");
      }

      return nextTxNum;
   }
}
