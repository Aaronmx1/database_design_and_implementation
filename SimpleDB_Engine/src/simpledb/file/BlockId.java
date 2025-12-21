package simpledb.file;

/**
 * AM: BlockId is the address of a unit being retrieved from Disk.
 */

public class BlockId {
   private String filename;
   private int blknum;

   public BlockId(String filename, int blknum) {
      this.filename = filename;
      this.blknum   = blknum;
   }

   public String fileName() {
      return filename;
   }

   public int number() {
      return blknum;
   }
   
   // AM: Required for use in HashMap
   public boolean equals(Object obj) {
      BlockId blk = (BlockId) obj;
      return filename.equals(blk.filename) && blknum == blk.blknum;
   }
   
   public String toString() {
      return "[file " + filename + ", block " + blknum + "]";
   }
   
   // AM: Required for use in HashMap
   public int hashCode() {
      return toString().hashCode();
   }
}
