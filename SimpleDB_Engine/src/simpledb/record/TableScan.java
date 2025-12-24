package simpledb.record;

import static java.sql.Types.INTEGER;
import simpledb.file.BlockId;
import simpledb.query.*;
import simpledb.tx.Transaction;

/**
 * Provides the abstraction of an arbitrarily large array
 * of records.
 * @author sciore
 */
/**
 * AM - Hybrid personal & AI write-up architecture 
 * The TableScan class acts as the "Project Manager" or "Iterator" for a database table.
 * It provides the abstraction of an infinite list of records, hiding the physical
 * reality of disjointed disk blocks.
 * * ARCHITECTURE OVERVIEW:
 * * 1. THE ABSTRACTION LAYER (The "Iterator")
 * - Clients (like the Query Planner) do not want to deal with BlockIds, Pages, or Pins.
 * - TableScan hides this complexity. It presents a simple API: beforeFirst(), next(),
 * getInt(), and setString().
 * - It makes a file spanning 100 blocks look like a single continuous array.
 * * 2. THE MANAGER (Managing the Worker)
 * - While TableScan manages the "File," it delegates the heavy lifting of managing
 * the "Block" to its worker: RecordPage.
 * - TableScan decides which block needs to be accessed, loads it, and then tells
 * RecordPage: "Go find the next empty slot on this page."
 * * 3. THE NAVIGATOR (Crossing Block Boundaries)
 * - The critical logic resides in the next() and insert() methods.
 * - When RecordPage reports "I am out of records on this block," TableScan takes over.
 * - It unpins the current block, calculates the next block number, pins the new block,
 * and sets up a new RecordPage worker to continue the work seamlessly.
 */
public class TableScan implements UpdateScan {
   private Transaction tx;
   private Layout layout;
   private RecordPage rp;
   private String filename;
   private int currentslot;

   public TableScan(Transaction tx, String tblname, Layout layout) {
      this.tx = tx;
      this.layout = layout;
      filename = tblname + ".tbl";
      if (tx.size(filename) == 0)
         moveToNewBlock();
      else 
         moveToBlock(0);
   }

   // Methods that implement Scan

   public void beforeFirst() {
      moveToBlock(0);
   }

   public boolean next() {
      currentslot = rp.nextAfter(currentslot);
      while (currentslot < 0) {
         if (atLastBlock())
            return false;
         moveToBlock(rp.block().number()+1);
         currentslot = rp.nextAfter(currentslot);
      }
      return true;
   }

   public int getInt(String fldname) {
      return rp.getInt(currentslot, fldname);
   }

   public String getString(String fldname) {
      return rp.getString(currentslot, fldname);
   }

   public Constant getVal(String fldname) {
      if (layout.schema().type(fldname) == INTEGER)
         return new Constant(getInt(fldname));
      else
         return new Constant(getString(fldname));
   }

   public boolean hasField(String fldname) {
      return layout.schema().hasField(fldname);
   }

   public void close() {
      if (rp != null)
         tx.unpin(rp.block());
   }

   // Methods that implement UpdateScan

   public void setInt(String fldname, int val) {
      rp.setInt(currentslot, fldname, val);
   }
   
   public void setString(String fldname, String val) {
      rp.setString(currentslot, fldname, val);
   }

   public void setVal(String fldname, Constant val) {
      if (layout.schema().type(fldname) == INTEGER)
         setInt(fldname, val.asInt());
      else
         setString(fldname, val.asString());
   }

   public void insert() {
      currentslot = rp.insertAfter(currentslot);
      while (currentslot < 0) {
         if (atLastBlock()) 
            moveToNewBlock();
         else 
            moveToBlock(rp.block().number()+1);
         currentslot = rp.insertAfter(currentslot);
      }
   }

   public void delete() {
      rp.delete(currentslot);
   }

   public void moveToRid(RID rid) {
      close();
      BlockId blk = new BlockId(filename, rid.blockNumber());
      rp = new RecordPage(tx, blk, layout);
      currentslot = rid.slot();
   }

   public RID getRid() {
      return new RID(rp.block().number(), currentslot);
   }

   // Private auxiliary methods
   
   // AM: Retrieve existing block and move to the first slot on that block
   private void moveToBlock(int blknum) {
      close();
      BlockId blk = new BlockId(filename, blknum);
      rp = new RecordPage(tx, blk, layout);
      currentslot = -1;
   }

   // AM: Appends new block and formats new block with empty slots aligned with layout of Schema
   private void moveToNewBlock() {
      close();
      BlockId blk = tx.append(filename);
      rp = new RecordPage(tx, blk, layout);
      rp.format();
      currentslot = -1;
   }

   private boolean atLastBlock() {
      return rp.block().number() == tx.size(filename) - 1;
   }
}
