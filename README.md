# hive-kudu
Hive Kudu Storage Handler, Input & Output format, Writable and SerDe

This hive serde for kudu basically is a wrapper of the kudu-mapreduce package.

Since kudu-mapreduce is written for the MRv2 API and Hive still runs against the old MRv1 (mapred) API, 
there really is a lot of wrapping going on.

Selecting from a kudu table is unreliable due to a race condition with the kudu client in the kudu-mapreduce package 
for kudu versions before 1.7.0
(https://gerrit.cloudera.org/c/8921/)

A lot of work was done already by Bimal Tandel
(https://github.com/BimalTandel/HiveKudu-Handler)

Some other stuff is copied from twitter's elephantbird project like most of the wrapping.

## Notes
- This SerDe at the moment only supports creating Hive Tables on top of already existing Kudu tables. Use another tool (e.g. impala) to create your table on Kudu first.
- Hive will write rows as Upserts to Kudu, inserting new Rows and potentially updating existing rows. Hive's ACID capabilities are not supported.
- the Hive Table should not have any (Hive) partitions, since those column fields are not available to the record writer.
  This is not a big issue, Hive should be able to push down any filtering on partition columns to the Kudu Storage layer. 
- Predicate Pushdown doesn't work right now, we need to retrieve the partition columns from kudu and store them in the HMS

## TODO
- investigate if we can make the storage format compatible to impala's (so impala and hive can share the table)
  this would probably require hive to recognize STORED AS KUDU as a keyword. Haven't found out where to do that.
  
- nice to have: delete (not really sure how to implement that)
- followup: make ACID work as much as possible - right now Hive's ACID concept still revolves very much around files on HDFS

- Allow hive partitioning that reflects Kudu's partitions (problem: hive inserts don't contain partition info 
  so we don't have access to the value of the partition fields in the writer/serializer)
  
- Creating kudu tables
- predicate pushdown for the IN () operator
- UNIT TESTS!!
- nice to have: SerDeStats (currently not available from kudu)
- use the constants from the kudu-mapreduce package instead of redefining them in HiveKuduConstants 
  (not exposed by the kudu-mapreduce package yet, apparently it is also not intended, see discussion in 
  https://gerrit.cloudera.org/#/c/8920/)


## Create a an External table on existing Kudu Table
```sql
DELETE JARS;
add jar hdfs:///user/cvaliente/lib/hive-kudu-1.0.0-all.jar;

USE cvaliente;
create external table cvaliente.kudutest
ROW FORMAT SERDE
  'HiveKuduSerDe'
stored by 'KuduStorageHandler'
TBLPROPERTIES(
  'kudu.table_name' = 'original_kudu_name',
  'kudu.master_addresses' = 'mgmt0.hadoop.trivago.com:7051,mgmt1.hadoop.trivago.com:7051,mgmt2.hadoop.trivago.com:7051'
);


describe formatted kudutest;

insert into kudutest values (1, 'a'), (2, 'b'), (3, 'a');
select * from kudutest;
```