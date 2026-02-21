# Databricks notebook source
# MAGIC %md
# MAGIC ###DataLake (Deltalake) + Lakehouse (Deltatables) - using Delta format (parquet+snappy+delta log)

# COMMAND ----------

# MAGIC %md
# MAGIC Delta Lake is an open-source storage framework that brings reliability, ACID transactions, and performance to data lakes. It sits on top of Parquet files and is most commonly used with Apache Spark and Databricks.<br>
# MAGIC Delta Lake & Deltalakhouse is the Core/Analytical storage layer behind Bronze–Silver–Gold (medallion) architectures.
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo-whitebackground.png" style="width:300px; float: right"/>
# MAGIC
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Creating our first Delta Lake table
# MAGIC
# MAGIC Delta is the default file and table format using Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ![](https://docs.databricks.com/aws/en/assets/images/well-architected-lakehouse-7d7b521addc268ac8b3d597bafa8cae9.png)

# COMMAND ----------

# DBTITLE 1,Cell 4
# MAGIC %sql
# MAGIC drop table if exists lakehousecat1.deltadb.customer_txn;
# MAGIC drop table if exists lakehousecat1.deltadb.customer_txn_part;
# MAGIC drop table if exists lakehousecat1.deltadb.drugstbl;
# MAGIC drop table if exists lakehousecat1.deltadb.drugstbl_merge;
# MAGIC drop table if exists lakehousecat1.deltadb.drugstbl_partitioned;
# MAGIC drop table if exists lakehousecat1.deltadb.employee_dv_demo1;
# MAGIC drop table if exists lakehousecat1.deltadb.product_inventory;
# MAGIC drop table if exists lakehousecat1.deltadb.tblsales;

# COMMAND ----------

#spark.sql(f"drop catalog if exists lakehousecat1 cascade")
spark.sql(f"CREATE CATALOG IF NOT EXISTS lakehousecat1")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS lakehousecat1.deltadb;")
spark.sql(f"""CREATE VOLUME IF NOT EXISTS lakehousecat1.deltadb.datalake;""")
#spark.sql(f"""CREATE VOLUME IF NOT EXISTS lakehousecat1.deltadb.deltavolume;""")
#spark.sql(f"""CREATE VOLUME IF NOT EXISTS lakehousecat1.deltadb.deltavolume2;""")

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Write data into delta file (Datalake) and table (Lakehouse)
# MAGIC 1. How to migrate csv to delta format
# MAGIC 2. Difference between Delta and Parquet
# MAGIC 2. How to create Datalake & Lakehouse

# COMMAND ----------

#1. How to migrate csv to delta format (Delta Lake creation)
df = spark.read.csv('/Volumes/lakehousecat1/deltadb/datalake/druginfo.csv',header=True,inferSchema=True)#Reading normal data from datalake
df.write.format("delta").mode("overwrite").save("/Volumes/lakehousecat1/deltadb/datalake/targetdir")#writing normal data into deltalake(deltalake)
#2. Difference between Delta and Parquet
df.write.format("parquet").mode("overwrite").save("dbfs:/Volumes/lakehousecat1/deltadb/datalake/targetdirparquet")#writing normal data into parquet(datalake)
df.write.mode("overwrite").save("/Volumes/lakehousecat1/deltadb/datalake/targetdirdefaultdeltaparquet")#Databricks default format is delta(parquet)
#3. How to create Delta Lakehouse
spark.sql("drop table if exists lakehousecat1.deltadb.drugstbl")
df.write.saveAsTable("lakehousecat1.deltadb.drugstbl",mode='overwrite')#writing normal data from deltalakehouse(lakehouse)
#behind it stores the data in deltafile format in the s3 bucket (location is hidden for us in databricks free edition)

# COMMAND ----------

# MAGIC %sql
# MAGIC --under the hood data is stored in S3
# MAGIC explain select * from lakehousecat1.deltadb.drugstbl

# COMMAND ----------

# MAGIC %md
# MAGIC #####We can have schema evolution performed

# COMMAND ----------

#We can add Schema evolution feature just by adding the below option in Delta tables.
#df.write.option("mergeSchema","True").saveAsTable("lakehousecat1.deltadb.drugstbl",mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. DML Operations in Delta Tables & Files
# MAGIC - We are overcoming the WORM (Write Once Read Many) limitation in Cloud S3/GCS/ADLS or in Distributed storage layers like HDFS
# MAGIC - Delta file/table supports WMRM(Write Manay Read Many) operations, using DMLs such as  INSERT/DELETE/UPDATE/MERGE

# COMMAND ----------

# MAGIC %sql
# MAGIC --DDL is supportive (we will do more of these further)
# MAGIC create or replace table lakehousecat1.deltadb.sampletable(id int,name string) 
# MAGIC using delta;
# MAGIC insert into lakehousecat1.deltadb.sampletable values(1,'irfan');--Though the data is stored internally in delta file, we can't see the data in delta format in databricks serverless
# MAGIC describe history lakehousecat1.deltadb.sampletable;

# COMMAND ----------

# MAGIC %sql
# MAGIC use lakehousecat1.deltadb

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY lakehousecat1.deltadb.drugstbl

# COMMAND ----------

# MAGIC %sql
# MAGIC --DQL is supported
# MAGIC SELECT * FROM drugstbl where uniqueid=163740;

# COMMAND ----------

# MAGIC %md
# MAGIC #####a. Table Update

# COMMAND ----------

# MAGIC %sql
# MAGIC --DML - update is possible in the delta tables/files
# MAGIC UPDATE drugstbl
# MAGIC   SET rating=rating-1
# MAGIC where uniqueid=163740;

# COMMAND ----------

# MAGIC %sql
# MAGIC --default latest version will be shown
# MAGIC SELECT * FROM drugstbl 
# MAGIC where uniqueid=163740;

# COMMAND ----------

# MAGIC %md
# MAGIC #####b. Table Delete

# COMMAND ----------

# MAGIC %sql
# MAGIC --DML - Delete is possible on delta tables/files
# MAGIC DELETE FROM drugstbl
# MAGIC where uniqueid=163740;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM drugstbl
# MAGIC where uniqueid in (163740,206473);

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history drugstbl;

# COMMAND ----------

# MAGIC %md
# MAGIC #####c. File DML (Update/Delete)
# MAGIC We don't do file DML usually, we are doing here just for learning about 
# MAGIC - file also can be undergone with limited DML operation
# MAGIC - we need to learn about how the background delta operation is happening when i do DML

# COMMAND ----------

spark.read.format('delta').load('/Volumes/lakehousecat1/deltadb/datalake/targetdir').where('uniqueid=163740').show()

# COMMAND ----------

#DML on Files: How to update delta files (Not used very frequently)
from delta.tables import DeltaTable
deltafile = DeltaTable.forPath(spark, "/Volumes/lakehousecat1/deltadb/datalake/targetdir")
deltafile.update("uniqueid=163740", { "rating": "rating - 1" } )

# COMMAND ----------

spark.read.format('delta').load('/Volumes/lakehousecat1/deltadb/datalake/targetdir').where('uniqueid=163740').show()

# COMMAND ----------

# MAGIC %md
# MAGIC #####d. File Delete

# COMMAND ----------

df=spark.read.format("delta").load('/Volumes/lakehousecat1/deltadb/datalake/targetdir')
df.where('uniqueid=206473').show()

# COMMAND ----------

from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark, "/Volumes/lakehousecat1/deltadb/datalake/targetdir")
deltaTable.delete("uniqueid=206473")

# COMMAND ----------

#Latest version data will be queried by default
df=spark.read.format("delta").load('/Volumes/lakehousecat1/deltadb/datalake/targetdir')
df.where('uniqueid=206473').show()

# COMMAND ----------

# MAGIC %md
# MAGIC #####d. Merge Operation

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from drugstbl;

# COMMAND ----------

# MAGIC %sql
# MAGIC --CTAS (Create table As Select)
# MAGIC create or replace table drugstbl_merge as select * from drugstbl where rating<=8;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from drugstbl_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC --merge syntax
# MAGIC --merge into targetable using sourcetable on condition_to_join
# MAGIC --when matched then update
# MAGIC --when not matched then insert
# MAGIC --when not matched by source then delete (some additional data in the target should be deleted, which is already deleted in source)
# MAGIC --Delta table support merge operation for (insert/update/delete)
# MAGIC --2899 updated
# MAGIC --2801 inserted
# MAGIC MERGE INTO drugstbl_merge tgt--2899
# MAGIC USING drugstbl src--5700
# MAGIC ON tgt.uniqueid = src.uniqueid
# MAGIC WHEN MATCHED THEN--2899 update
# MAGIC   UPDATE SET tgt.usefulcount= src.usefulcount,
# MAGIC              tgt.drugname = src.drugname,
# MAGIC              tgt.condition = src.condition
# MAGIC WHEN NOT MATCHED--2801 insert
# MAGIC   THEN INSERT (uniqueid,rating,date,usefulcount, drugname, condition ) VALUES (uniqueid,rating,date,usefulcount, drugname, condition);
# MAGIC   --5700-2899 = 2801

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from drugstbl_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC --What if the source got some data removed and which is present in the target still (we can leave it or delete)
# MAGIC insert into drugstbl_merge select 99999999,drugname,condition,rating,date,usefulcount 
# MAGIC from drugstbl limit 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Target table contains excessive data
# MAGIC select count(*) from drugstbl_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Source table got few data deleted
# MAGIC select count(*) from drugstbl;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Delta table support merge operation for (delete)
# MAGIC --1 deleted (which is not present in the source (source system deleted it already, hence target also has to delete))
# MAGIC MERGE INTO drugstbl_merge tgt
# MAGIC USING drugstbl src
# MAGIC ON tgt.uniqueid = src.uniqueid
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.usefulcount= src.usefulcount,
# MAGIC              tgt.drugname = src.drugname,
# MAGIC              tgt.condition = src.condition
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (uniqueid,rating,date,usefulcount, drugname, condition ) VALUES (uniqueid,rating,date,usefulcount, drugname, condition)
# MAGIC WHEN NOT MATCHED BY SOURCE THEN DELETE;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from drugstbl_merge;

# COMMAND ----------

#Few points to consider regarding merge...
#1. Merge can be only applied on tables in Databricks delta
#2. Merge operation using spark with (library delta.tables.DeltaTable) DSL (not by using SQL) - SQL is better to use
from delta.tables import DeltaTable
print(spark.read.table("drugstbl").count())
print(spark.read.table("drugstbl_merge").count())
tgt = DeltaTable.forName(spark, "drugstbl_merge")
src = spark.table("drugstbl")
(
    tgt.alias("tgt")
    .merge(
        src.alias("src"),
        "tgt.uniqueid = src.uniqueid"
    )
    .whenMatchedUpdate(set={
        "usefulcount": "src.usefulcount",
        "drugname": "src.drugname",
        "condition": "src.condition"
    })
    .whenNotMatchedInsert(values={
        "uniqueid": "src.uniqueid",
        "rating": "src.rating",
        "date": "src.date",
        "usefulcount": "src.usefulcount",
        "drugname": "src.drugname",
        "condition": "src.condition"
    })
    .whenNotMatchedBySourceDelete()
    .execute() )


# COMMAND ----------

print(spark.read.table("drugstbl").count())
print(spark.read.table("drugstbl_merge").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Additional Operations on Deltalake & Deltatables

# COMMAND ----------

# MAGIC %md
# MAGIC #####a. History & Versioning
# MAGIC *History* returns one row per commit/version and tells you what changed, when, and how.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY drugstbl_merge

# COMMAND ----------

# MAGIC %md
# MAGIC *Version as of* will reads the snapshot of drugstbl_merge at version 4 and Ignores all changes made in versions 5, 6, … current

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from (select * from deltadb.drugs version as of 2) where uniqueid=163740;
# MAGIC SELECT count(*) FROM drugstbl_merge VERSION AS OF 2;--Behind the scene, databricks sql engine with the help of deltaengine (it will read the log and the respective data and produce the output)

# COMMAND ----------

# MAGIC %md
# MAGIC #####b. Time Travel
# MAGIC *Timestamp as of* Reads the table as it existed at that exact timestamp and Any commits after the given timestamp is ignored

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1) FROM drugstbl_merge TIMESTAMP AS OF '2026-01-31T07:18:52.000+00:00';

# COMMAND ----------

# MAGIC %md
# MAGIC #####c. Vaccum
# MAGIC *VACUUM* in Delta Lake removes old, unused files to free up storage, default retention hours is 168. These files come from operations like DELETE, UPDATE, or MERGE and are kept temporarily so time-travel queries can work.<br>
# MAGIC
# MAGIC Before VACUUM<br>
# MAGIC Active + deleted parquet files exist<br>
# MAGIC
# MAGIC After VACUUM<br>
# MAGIC Only ACTIVE parquet files remains and delete Old parquet files (from UPDATE/MERGE/DELETE)<br>
# MAGIC Logs remain intact<br>
# MAGIC Time travel beyond retention becomes impossible<br>

# COMMAND ----------

# MAGIC %sql
# MAGIC --use lakehousecat1.deltadb;
# MAGIC DESC HISTORY prodcatalog.logistics.silver_staff;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from prodcatalog.logistics.silver_staff TIMESTAMP AS OF '2026-01-23T03:24:19.000+00:00';

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table prodcatalog.logistics.silver_staff SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = '24 hours');
# MAGIC VACUUM prodcatalog.logistics.silver_staff;--default value in databricks, we can reduce or increase this(but in serverless it is not possible to reduce)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1) FROM drugstbl_merge TIMESTAMP AS OF '2026-01-23T03:24:19.000+00:00';

# COMMAND ----------

# MAGIC %md
# MAGIC AI Suggested feature<br>
# MAGIC Setting the Delta table property 'delta.deletedFileRetentionDuration' to less than the default (1 week) is generally not recommended for production environments. Lowering the retention duration can lead to data loss if you need to time travel or restore data, as older files may be deleted sooner than expected. The default of 168 hours (1 week) is chosen to balance storage costs and safety for production workloads. Only reduce this value if you fully understand the risks and have a strong operational reason to do so

# COMMAND ----------

# MAGIC %sql
# MAGIC --These properties can't be set in serverless, we will see this in cluster or i will get a table with more than 1 week data
# MAGIC --SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC --alter table drugstbl_merge SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = '24 hours');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --use lakehousecat1.deltadb;
# MAGIC DESC HISTORY drugstbl_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC --default 168 hours (1 week), less than 1 week will not work in serverless for performance and session state reason
# MAGIC VACUUM drugstbl_merge RETAIN 168 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1) FROM drugstbl_merge TIMESTAMP AS OF '2026-01-26T16:55:40.000+00:00';

# COMMAND ----------

spark.sql("VACUUM drugstbl_merge RETAIN 168 HOURS")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY drugstbl_merge

# COMMAND ----------

# MAGIC %md
# MAGIC #####d. ACID Transactions
# MAGIC **Delta Lake supports ACID transactions under the hood via a transaction log.**
# MAGIC | ACID        | In Databricks         |
# MAGIC | ----------- | --------------------- |
# MAGIC | Atomicity   | Every transactions are Individual Transactions / All or nothing  |
# MAGIC | Consistency | Schema + constraints  |
# MAGIC | Isolation   | Using Version/Time/restore we can isolate transactions, we can't use TCL (commit/rollback) |
# MAGIC | Durability  | Every transaction Always hit the disk (durable), but can be controlled by Transaction log |

# COMMAND ----------

# MAGIC %sql
# MAGIC use lakehousecat1.deltadb

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history acid_demo_txn 

# COMMAND ----------

# DBTITLE 1,Untitled
# MAGIC %sql
# MAGIC select * from acid_demo_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE acid_demo_txn (
# MAGIC   id INT,
# MAGIC   amount INT
# MAGIC ) USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acid_demo_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC --All or nothing (Atomic/Individual Transaction) - help us make a transaction complete or fail, hence no partial data
# MAGIC INSERT INTO acid_demo_txn VALUES--One atomic (all or nothing) transaction is inserting 3 rows
# MAGIC (1, 100),
# MAGIC (2, 200),
# MAGIC (3, 300);
# MAGIC --(3, '300');
# MAGIC --This will make the entire transaction failed (all or nothing)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acid_demo_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acid_demo_txn where id=1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Atomicity (Individual transaction that doesn't affect the other)
# MAGIC UPDATE acid_demo_txn SET amount = amount + 100 WHERE id = 1;--individual/atomic
# MAGIC --The above statement is atomic, hence the below statement take amount as 200 and added 200 more
# MAGIC UPDATE acid_demo_txn SET amount = amount + 200 WHERE id = 1;--individual/atomic
# MAGIC describe history acid_demo_txn;
# MAGIC --START TRANSACTION; UPDATE acid_demo_txn SET amount = amount + 100 WHERE id = 1; commit;
# MAGIC --START TRANSACTION; UPDATE acid_demo_txn SET amount = amount + 200 WHERE id = 1; commit;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acid_demo_txn where id=1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Apply constraint for maintaining consistancy
# MAGIC --We can apply in databricks deltatable, 2 types of constraints (check and not null), 
# MAGIC -- in other DBs we can use primary key, foreign key and unique constraints also..
# MAGIC ALTER TABLE acid_demo_txn ADD CONSTRAINT positive_amount CHECK (amount > 0);
# MAGIC INSERT INTO acid_demo_txn VALUES (4, 100);--Atomicity and consistancy
# MAGIC INSERT INTO acid_demo_txn VALUES (5, -100);--Atomicity and consistancy

# COMMAND ----------

# MAGIC %sql
# MAGIC --Only consistant data is loaded
# MAGIC select * from acid_demo_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Isolation (We can achieve using timetravel (restore operation (no rollback)))
# MAGIC --START TRANSACTION; SAVEPOINT before_delete; DELETE FROM employees WHERE employee_id = 129; ROLLBACK TO before_delete;
# MAGIC --Notebook1 (We can see the data in notebook1)
# MAGIC UPDATE acid_demo_txn SET amount = 999 WHERE id = 2;--This update will write the data in the disk with version added
# MAGIC --Notebook2 (We can see the data in notebook2 )
# MAGIC --use lakehousecat1.deltadb;
# MAGIC --select * from (select * from acid_demo_txn version as of 14) where id=2;--serializable read (after the data successfully committed)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from acid_demo_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC --something like savepoint+rollback (but not really a rollback (TCL is not available in Databricks in the name of commit, rollback, savepoint))
# MAGIC restore table acid_demo_txn to version as of 9;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history acid_demo_txn;
# MAGIC --select * from acid_demo_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Durability (Despite of terminate and starting back the serverless, data still survives durably)
# MAGIC INSERT INTO acid_demo_txn VALUES (5, 500);

# COMMAND ----------

# MAGIC %sql
# MAGIC use lakehousecat1.deltadb;
# MAGIC select * from acid_demo_txn;

# COMMAND ----------

# MAGIC %md
# MAGIC #####e. Transactions Control (TCL cannot be achieved using commit/rollback/savepoint)
# MAGIC Bigdata ecosystems such as spark/databricks/delta are not Transaction in nature, hence it will not support TCL directly, but can be achieved using version/timetravel/restore

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC --select count(1) from deltadb.drugs where date>'2012-02-28';
# MAGIC --4329
# MAGIC --Equivalent to delete and commit (with version (savepoint))
# MAGIC delete from drugstbl where date>'2012-02-28';

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(1) from drugstbl;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history drugstbl;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from drugstbl;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Equivalent to restore to a version
# MAGIC --Equivalent to Rollback to a savepoint
# MAGIC RESTORE TABLE drugstbl TO VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from drugstbl;

# COMMAND ----------

# MAGIC %sql
# MAGIC --We can restore to any older/later version (unlit 168 hours/vacumm period)
# MAGIC RESTORE TABLE drugstbl TO VERSION AS OF 4;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history drugstbl;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from drugstbl;
