package simpledb.tx.recovery;

import static simpledb.tx.recovery.LogRecord.CHECKPOINT;
import static simpledb.tx.recovery.LogRecord.COMMIT;
import static simpledb.tx.recovery.LogRecord.ROLLBACK;
import static simpledb.tx.recovery.LogRecord.START;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.BlockId;
import simpledb.log.LogMgr;
import simpledb.tx.Transaction;

/**
 * The recovery manager.  Each transaction has its own recovery manager.
 * @author Edward Sciore
 */

/**
 * AM - Hybrid personal & AI write-up architecture 
 * The RecoveryMgr class handles the Atomicity and Durability properties of ACID.
 * Each Transaction has its own unique RecoveryMgr instance.
 * * ARCHITECTURE OVERVIEW:
 * * 1. THE SCRIBE (Write-Ahead Logging)
 * - Before any data is modified in memory (Buffer), this manager writes a
 * description of the change (LogRecord) to the Log.
 * - This ensures that if the system crashes, we have a "receipt" of what we intended to do.
 * - Supports specific record types: START, COMMIT, ROLLBACK, CHECKPOINT, SETINT, SETSTRING.
 * * 2. THE SAFETY NET (Rollback)
 * - If a transaction fails (or the user cancels it), this manager reads the Log backwards.
 * - It finds the specific changes made by this transaction ID and undoes them
 * (restoring old values).
 * * 3. THE RESTORER (Crash Recovery)
 * - On system startup, this manager performs the "Undo" phase of the recovery algorithm.
 * - It scans the log to find transactions that were running when the crash happened
 * (i.e., they have a START but no COMMIT/ROLLBACK).
 * - It undoes all changes for those "loser" transactions to ensure the database
 * returns to a consistent state.
 */

/**
 * AM: RecoverMgr focuses at the Transaction level
 */
public class RecoveryMgr {
   private LogMgr lm;
   private BufferMgr bm;
   private Transaction tx;
   private int txnum;

   /**
    * Create a recovery manager for the specified transaction.
    * @param txnum the ID of the specified transaction
    */
   public RecoveryMgr(Transaction tx, int txnum, LogMgr lm, BufferMgr bm) {
      this.tx = tx;
      this.txnum = txnum;
      this.lm = lm;
      this.bm = bm;
      StartRecord.writeToLog(lm, txnum);
   }

   /**
    * Write a commit record to the log, and flushes it to disk.
    */
   /**
    * AM: User Data Pages are flushed first before writing Commit to the Log to simplify recovery.
    */
   public void commit() {
      bm.flushAll(txnum);                             // AM: Flushes Log (the actual "SETINT" changes) and Buffer w/ modified data to the Disk.
      int lsn = CommitRecord.writeToLog(lm, txnum);   // AM: Writes the Commit Record to the Log.
      lm.flush(lsn);                                  // AM: Flushes CommitRecord update (ie. Commit Log record) to Disk.
   }

   /**
    * Write a rollback record to the log and flush it to disk.
    */
   public void rollback() {
      doRollback();
      bm.flushAll(txnum);                             // AM: Flushes rolled-back Buffer of transaction to the Log and Disk
      int lsn = RollbackRecord.writeToLog(lm, txnum);
      lm.flush(lsn);
   }

   /**
    * Recover uncompleted transactions from the log
    * and then write a quiescent checkpoint record to the log and flush it.
    */
   public void recover() {
      doRecover();
      bm.flushAll(txnum);
      int lsn = CheckpointRecord.writeToLog(lm);
      lm.flush(lsn);
   }

   /**
    * Write a setint record to the log and return its lsn.
    * @param buff the buffer containing the page
    * @param offset the offset of the value in the page
    * @param newval the value to be written
    */
   public int setInt(Buffer buff, int offset, int newval) {
      int oldval = buff.contents().getInt(offset);
      BlockId blk = buff.block();
      return SetIntRecord.writeToLog(lm, txnum, blk, offset, oldval);
   }

   /**
    * Write a setstring record to the log and return its lsn.
    * @param buff the buffer containing the page
    * @param offset the offset of the value in the page
    * @param newval the value to be written
    */
   public int setString(Buffer buff, int offset, String newval) {
      String oldval = buff.contents().getString(offset);                   // 1. Read the OLD value currently in the buffer
      BlockId blk = buff.block();
      return SetStringRecord.writeToLog(lm, txnum, blk, offset, oldval);   // 2. Write the OLD value to the Log
   }

   /**
    * Rollback the transaction, by iterating
    * through the log records until it finds 
    * the transaction's START record,
    * calling undo() for each of the transaction's
    * log records.
    */
   private void doRollback() {
      Iterator<byte[]> iter = lm.iterator();
      while (iter.hasNext()) {
         byte[] bytes = iter.next();
         LogRecord rec = LogRecord.createLogRecord(bytes); 
         if (rec.txNumber() == txnum) {
            if (rec.op() == START)
               return;
            rec.undo(tx);
         }
      }
   }

   /**
    * Do a complete database recovery.
    * The method iterates through the log records.
    * Whenever it finds a log record for an unfinished
    * transaction, it calls undo() on that record.
    * The method stops when it encounters a CHECKPOINT record
    * or the end of the log.
    */
   private void doRecover() {
      Collection<Integer> finishedTxs = new ArrayList<>();
      Iterator<byte[]> iter = lm.iterator();
      while (iter.hasNext()) {
         byte[] bytes = iter.next();
         LogRecord rec = LogRecord.createLogRecord(bytes);
         if (rec.op() == CHECKPOINT)
            return;
         if (rec.op() == COMMIT || rec.op() == ROLLBACK)
            finishedTxs.add(rec.txNumber());             // AM: Step 1: Mark Transaction as "Done"
         else if (!finishedTxs.contains(rec.txNumber()))
            rec.undo(tx);                                // AM: Step 2: Only undo if NOT "Done"
      }
   }
}
