package simpledb.file;

import java.io.*;
import java.util.*;

import javax.management.RuntimeErrorException;

/* Handles the interaction with the OS file system. */
/**
 * AM - Hybrid personal & AI write-up architecture
 * The FileMgr class is the "Physical Layer" of the database engine.
 * It is the only class that actually touches the hard drive (via OS calls).
 * * ARCHITECTURE OVERVIEW:
 * * 1. THE TRANSLATOR (Logical -> Physical)
 * - Upper layers think in "Blocks" (e.g., "Block 5 of file 'student.tbl'").
 * - FileMgr translates this into physical byte offsets (e.g., "Byte 20,480").
 * - Formula: Physical Position = Block Number * Block Size.
 * * 2. THE BRIDGE (Disk <-> Page)
 * - read(): Pours bytes from the Disk Channel into a Page memory buffer.
 * - write(): Pours bytes from a Page memory buffer into the Disk Channel.
 * * 3. SYNCHRONIZATION
 * - All I/O methods are 'synchronized'.
 * - This prevents race conditions where two threads try to move the file cursor (seek)
 * of the same file at the same time.
 * * 4. FILE EXTENSION & TRUNCATION
 * - append(): grows the file when new space is needed.
 * - truncate(): (Exercise 5.48) shrinks the file during rollback to prevent data bloat.
 */
public class FileMgr {
   private File dbDirectory;
   private int blocksize;                             // AM: Size of a Block read in from Disk. The Page size will be equivalent to Block size.
   private boolean isNew;
   private Map<String,RandomAccessFile> openFiles = new HashMap<>();
   private int blockStatistics;                       // AM: Maintains block tracking statistics

   public FileMgr(File dbDirectory, int blocksize) {
      this.dbDirectory = dbDirectory;
      this.blocksize = blocksize;
      isNew = !dbDirectory.exists();

      // create the directory if the database is new
      if (isNew)
         dbDirectory.mkdirs();

      // remove any leftover temporary tables
      for (String filename : dbDirectory.list())
         if (filename.startsWith("temp"))
         		new File(dbDirectory, filename).delete();
   }

   /* AM: read(),  write(), and append() are synchronized to allow only one thread to be executing them at a time.
    * This is needed to maintain consistency when methods share updateable objects, such as RandomAccessFile objects.

   *  AM: read()
   * [ Disk / File ]  ------ Data Flows THIS Way ----->  [ RAM / Page ]
         ^                                                   ^
         |                                                   |
      The Channel                                       The Buffer
      (The Source)                                   (The Destination)
   */

   public synchronized void read(BlockId blk, Page p) {
      try {
         RandomAccessFile f = getFile(blk.fileName());   // AM: Handles the actual I/O operations and positioning within the File.
         f.seek(blk.number() * blocksize);
         f.getChannel().read(p.contents());              // AM: Read data from File (the channel) and insert into the Page. FileMgr pours bytes from disk directly into Page object which is passed-by-reference.  Source = Disk File, Destination = Page (ByteBuffer).
         trackBlocks();
      }
      catch (IOException e) {
         throw new RuntimeException("cannot read block " + blk);
      }
   }

   /* AM: write()
   *  [ Disk / File ]  <----- Data Flows THIS Way ------  [ RAM / Page ]
            ^                                                   ^
            |                                                   |
         The Channel                                       The Buffer
      (The Destination)                                    (The Source)
    */

   public synchronized void write(BlockId blk, Page p) {
      try {
         RandomAccessFile f = getFile(blk.fileName());   // AM: RandomAccessFile acts like a cursor that lets you point to a specific location in that file (ie. a vinyl record arm that can be moved to any part of the record).  RandomAccessFile is the bridge to the OS.
         f.seek(blk.number() * blocksize);               // AM: Moves cursor of RandomAccessFile to a specific point in File (ie. block number * blocksize)
         f.getChannel().write(p.contents());             // AM: Write data to the file (the channel) by taking it from the Buffer. Source = Page(ByteBuffer), Destination = Disk file
         trackBlocks();                                  // AM: track blocks
      }
      catch (IOException e) {
         throw new RuntimeException("cannot write block" + blk);
      }
   }

   public synchronized BlockId append(String filename) {
      int newblknum = length(filename);                     // AM: Existing file size / block size = new block number
      BlockId blk = new BlockId(filename, newblknum);
      byte[] b = new byte[blocksize];
      try {
         RandomAccessFile f = getFile(blk.fileName());      // AM: getFile() checks if openFiles hashmap contains that blockId filename; if not, it creates a new RandomAccessFile object to open the connection to the existing disk file.
         f.seek(blk.number() * blocksize);                  // AM: Calculates seek position to locate contents of block.
         f.write(b);
         trackBlocks();                                     // AM: track blocks
      }
      catch (IOException e) {
         throw new RuntimeException("cannot append block" + blk);
      }
      return blk;
   }

   public int length(String filename) {
      try {
         RandomAccessFile f = getFile(filename);
         return (int)(f.length() / blocksize);              // AM: The same file "filename" is used and grows over time leading to new BlockId's being generated.
      }
      catch (IOException e) {
         throw new RuntimeException("cannot access " + filename);
      }
   }

   public boolean isNew() {
      return isNew;
   }
   
   public int blockSize() {
      return blocksize;
   }

   private RandomAccessFile getFile(String filename) throws IOException {
      RandomAccessFile f = openFiles.get(filename);         // AM: Looks for filename in HashMap dictionary
      if (f == null) {
         File dbTable = new File(dbDirectory, filename);    // AM: Create dbTable object with the structure dbDirectory (parent) and filename (child)
         f = new RandomAccessFile(dbTable, "rws");          // AM: Establishes a connection to the physical file on Disk, represented as a FileDescriptor. It allows manipulation of a "file pointer" to move to a specific byte position for reading/writing w/o reading the entire file into memory at once. "rws" = Read, Write, Synchronous.
         openFiles.put(filename, f);                        // AM: Store filename inside dbTable directory
      }
      return f;
   }

   // AM: Track read/write block statistics
   private void trackBlocks(){
      blockStatistics = blockStatistics + 1;
   }

   public void resetBlockStatistics(){
      blockStatistics = 0;
   }

   // AM: Return block statistics
   public int getBlockStatistics(){
      return blockStatistics;
   }

   // AM: Exercise 5.48 - Adding truncate to remove extra blocks created during append(), but rolled-back
   public synchronized void truncate(String filename, int blknum){
      try{
         RandomAccessFile f = getFile(filename);
         f.setLength(blknum * blocksize);          // AM: Chops the file
      }
      catch(IOException e){
         throw new RuntimeException("cannot truncate file "+ filename);
      }
   }

}
