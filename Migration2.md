
###From abstract topics to real table names
 
  - stop scheduler
  - stop kafka
  - download data for Lineage, Offsets, OffsetsLocal (both) to sheets
  - make changes in sheets (ask Daniel)
  - upload back
  - modify MV code to new Offsets topic names (db.table)
  - apply new code to SCH dir
    - Lineage
    - Offsets
    - ProcessTemplate
    - LagLive
    - Log view to MV for Reload entries
  - start kafka
  - start scheduler (no changes for scheduler!)
  - Check
    - Offsets is moving
    - Scheduler errors
    - daily volume in Fact tables

