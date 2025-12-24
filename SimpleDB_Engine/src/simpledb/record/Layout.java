package simpledb.record;

import java.util.*;
import static java.sql.Types.*;
import simpledb.file.Page;

/**
 * Description of the structure of a record.
 * It contains the name, type, length and offset of
 * each field of the table.
 * @author Edward Sciore
 *
 */
/**
 * AM - Hybrid personal & AI write-up architecture 
 * The Layout class acts as the "Tape Measure" for the Record Manager.
 * It translates the logical Schema into physical byte offsets within a disk block.
 * * ARCHITECTURE OVERVIEW:
 * * 1. THE PHYSICAL MAPPER
 * - It takes the logical 'Schema' and calculates exactly how many bytes each
 * field consumes (e.g., INTEGER = 4 bytes).
 * - It determines the exact starting position (offset) of every field within a record.
 * * 2. SLOT STRUCTURE ENFORCEMENT
 * - It defines the "Slot": A fixed-size region of bytes capable of holding one record.
 * - It automatically reserves space (Bytes 0-3) at the start of every slot for
 * the "Empty/Used" status flag.
 * - It calculates the total 'slotSize' (Record Data + Flag Overhead) to allow
 * mathematical jumping from Record 0 to Record 1 (pos = slot * slotSize).
 * * 3. THE BRIDGE
 * - It bridges the gap between the Schema (which knows "Name") and the
 * RecordPage (which needs "Byte 32").
 */
public class Layout {
   private Schema schema;
   private Map<String,Integer> offsets;      // AM: Maintains field name and it's length in bytes
   private int slotsize;

   /**
    * This constructor creates a Layout object from a schema. 
    * This constructor is used when a table 
    * is created. It determines the physical offset of 
    * each field within the record.
    * @param tblname the name of the table
    * @param schema the schema of the table's records
    */
   public Layout(Schema schema) {
      this.schema = schema;
      offsets  = new HashMap<>();
      int pos = Integer.BYTES; // leave space for the empty/inuse flag
      for (String fldname : schema.fields()) {
         offsets.put(fldname, pos);
         pos += lengthInBytes(fldname);
      }
      slotsize = pos;
   }

   /**
    * Create a Layout object from the specified metadata.
    * This constructor is used when the metadata
    * is retrieved from the catalog.
    * @param tblname the name of the table
    * @param schema the schema of the table's records
    * @param offsets the already-calculated offsets of the fields within a record
    * @param recordlen the already-calculated length of each record
    */
   public Layout(Schema schema, Map<String,Integer> offsets, int slotsize) {
      this.schema    = schema;
      this.offsets   = offsets;
      this.slotsize = slotsize;
   }

   /**
    * Return the schema of the table's records
    * @return the table's record schema
    */
   public Schema schema() {
      return schema;
   }

   /**
    * Return the offset of a specified field within a record
    * @param fldname the name of the field
    * @return the offset of that field within a record
    */
   public int offset(String fldname) {
      return offsets.get(fldname);
   }

   /**
    * Return the size of a slot, in bytes.
    * @return the size of a slot
    */
   public int slotSize() {
      return slotsize;
   }

   private int lengthInBytes(String fldname) {
      int fldtype = schema.type(fldname);
      if (fldtype == INTEGER)
         return Integer.BYTES;
      else // fldtype == VARCHAR
         return Page.maxLength(schema.length(fldname));  // AM: INTEGER + Bytes for string objects
   }
}

