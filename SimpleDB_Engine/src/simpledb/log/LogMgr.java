package simpledb.log;

import java.util.Iterator;
import simpledb.file.*;
import simpledb.buffer.*;  // AM: imports BufferMgr and Buffer classes

/**
 * The log manager, which is responsible for 
 * writing log records into a log file. The tail of 
 * the log is kept in a bytebuffer, which is flushed
 * to disk when needed. 
 * @author Edward Sciore
 */
/**
 * AM - Hybrid personal & AI write-up architecture
 * The LogMgr class handles the "Durability" aspect of the database.
 * It is responsible for writing a sequential history of all database events.
 * * ARCHITECTURE OVERVIEW:
 * * 1. THE APPEND-ONLY STRUCTURE
 * - The Log is a "Append-Only" file. We never modify old records; we only add new ones.
 * - This creates an immutable history that RecoveryMgr uses to reconstruct state.
 * * 2. REVERSE STORAGE (Optimization)
 * - Records are written into the buffer from Right-to-Left (Backwards).
 * - Why? Because RecoveryMgr always reads the log Backwards (from most recent to oldest).
 * - Storing them in reverse order makes the iterator logic incredibly fast and simple.
 * * 3. MEMORY MANAGEMENT (Integration with BufferMgr)
 * - Instead of managing its own private memory array, LogMgr (post-Exercise 4.11)
 * now uses the system's BufferMgr to pin a specific "Log Page".
 * - This ensures the Log plays nicely with the global memory pool.
 * * 4. LOG SEQUENCE NUMBERS (LSN)
 * - Every log record is assigned a unique, increasing integer (LSN).
 * - This LSN serves as a timestamp and a pointer used by BufferMgr to ensure
 * "Write-Ahead Logging" (a log record must reach disk before the data page does).
 */
public class LogMgr {
   private FileMgr fm;
   private String logfile;
   private Page logpage;         // AM: logBuffer supercedes this variable since BufferMgr now handles Log's (see 4.11 Programming Exercise).
   private BlockId currentblk;
   private int latestLSN = 0;    // AM: LSN = Log Sequence Number
   private int lastSavedLSN = 0;

   // AM: Exercise 4.11 - Handle LogMgr in BufferMgr
   private BufferMgr bm;         // AM: Handles BufferMgr
   private Buffer logBuffer;     // AM: Handles currently pinned buffer for the log.

   /**
    * Creates the manager for the specified log file.
    * If the log file does not yet exist, it is created
    * with an empty first block.
    * @param FileMgr the file manager
    * @param logfile the name of the log file
    */

   public LogMgr(FileMgr fm, String logfile) {
      this.fm = fm;
      this.logfile = logfile;

      int logsize = fm.length(logfile);
      if (logsize == 0)
         currentblk = appendNewBlock();
      else {
         currentblk = new BlockId(logfile, logsize-1);
      }
   }

   /* AM: Exercise 4.11
    * Finds a spot in the Buffer Pool, loads the data, and returns the Buffer object to the LogMgr.
    */
   public void setBufferMgr(BufferMgr bm){
      this.bm = bm;
      logBuffer = bm.pin(currentblk);     // AM: Pinned Buffer object referencing file block
   }

   /**
    * Ensures that the log record corresponding to the
    * specified LSN has been written to disk.
    * All earlier log records will also be written to disk.
    * @param lsn the LSN of a log record
    */
   public void flush(int lsn) {
      if (lsn >= lastSavedLSN)
         flush();
   }

   public Iterator<byte[]> iterator(){
      flush();
      return new LogIterator(fm, currentblk, bm);
   }

   /**
    * Appends a log record to the log buffer. 
    * The record consists of an arbitrary array of bytes. 
    * Log records are written right to left in the buffer.
    * The size of the record is written before the bytes.
    * The beginning of the buffer contains the location
    * of the last-written record (the "boundary").
    * Storing the records backwards makes it easy to read
    * them in reverse order.
    * @param logrec a byte buffer containing the bytes.
    * @return the LSN of the final value
    */
   public synchronized int append(byte[] logrec) {      
      /* AM: Exercise 4.11 */
      // AM: 1. Access the Page via the Buffer
      Page p = logBuffer.contents();       // AM: Return reference to Log's Buffer Page

      int boundary = p.getInt(0);   // AM: Returns remaining space available in Page
      int recsize = logrec.length;         // AM: Returns Log record size
      int bytesneeded = recsize + Integer.BYTES; // AM: Total bytes needed to store record (i.e. record size + length of record) [eg. A string of "abc" & integer "101" will occupy 7 bytes and 4 bytes to record length for a total of 11 bytes to store the record]
                                                   /** AM: [Blob Len (4)] [String Len (4)] [String "abc" (3)] [Integer Val (4)]
                                                            |____________| |___________________________________________________|
                                                                  |                                  |
                                                            Added by LogMgr              The "record" created in LogTest
                                                    */

      // AM: Checks if current Log Buffer Page has enough room; if not, then a new Block is appended to the existing Log for additional space
      if(boundary - bytesneeded < Integer.BYTES){
         flush();
         currentblk = appendNewBlock();

         // AM: Switch buffers
         bm.unpin(logBuffer);             // AM: Release the full block
         logBuffer = bm.pin(currentblk);  // AM: Pin the new empty block

         p = logBuffer.contents();        // AM: Update the local page reference
         boundary = p.getInt(0);
      }

      /** AM: Visualizating the Block of a Log
       *    Offset 0       : [ 385 ]  <-- Header: Points to start of the most recent record
            Offset 4 - 384 : [ 0 0 0 ... ] (Empty Space)
            Offset 385     : [ 11 ]   <-- LOG MGR HEADER: Total size of the blob
            Offset 389     : [ 3 ]    <-- USER DATA: Length of string "abc"
            Offset 393     : [ a b c ] <-- USER DATA: The string
            Offset 396     : [ 101 ]  <-- USER DATA: The integer
      */
      // AM: Log record is populated from EOF to beginning (in reverse). Offset "0" maintains Block size to track remaining room available in Page.
      int recpos = boundary - bytesneeded;   // AM: Store position in Page where new record will be added
      p.setBytes(recpos, logrec);            // AM: Set Page by adding new Log Record at specified position
      p.setInt(0, recpos);           // AM: Update Page's record position to reflect new starting position
      latestLSN += 1;                        // AM: Update Log Sequence Number show a new log record has been added
      
      return latestLSN;
   }

   /**
    * Initialize the bytebuffer and append it to the log file.
    */
  /**
   * AM: Update the Page via the Buffer.
   *     If Block is full then a new Block will be created to handle Block and written to Disk.
   *     This Block is then pinned in the BufferMgr (during append()) and the BlockId is returned.
   */
   private BlockId appendNewBlock(){
      BlockId blk = fm.append(logfile);     // AM: Generates a BlockId reference to a File.

      Page p = new Page(fm.blockSize());    // AM: Create new Page of desired Block size
      p.setInt(0, fm.blockSize());  // AM: Sets the Block size of Page at offset "0".
      fm.write(blk, p);                     // AM: Writes the Block and Page to the File

      return blk;
   }

   /**
    * Write the buffer to the log file.
   */
  /* AM: Flush the Buffer to disk immediately.
   *     Use the FileMgr directly to avoid circular dependency logic in Buffer.flush()
   */ 
  private void flush(){
      fm.write(currentblk, logBuffer.contents());  // AM: Flush Log Buffer Block to Disk
      lastSavedLSN = latestLSN;     // AM: Update Log Sequence Number to most recent flushed LSN.
  }
}
