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

## To Do's 
- make selects work reliable after https://gerrit.cloudera.org/c/8921/ - solved in Kudu 1.7.0
- make ACID work as much as possible - right now Hive's ACID concept still revolves very much around files on HDFS
- Allow hive partitioning that reflects Kudu's partitions (problem: hive inserts don't contain partition info 
  so we don't have access to the value of the partition fields in the writer/serializer)
- Creating kudu table with range partitions (probably better to do that in impala and create an external Hive table)
- predicate pushdown for the IN () operator
- UNIT TESTS!!
- nice to have: SerDeStats (currently not available from kudu)
- nice to have: delete (not really sure how to implement that)
- use the constants from the kudu-mapreduce package instead of redefining them in HiveKuduConstants 
  (not exposed by the kudu-mapreduce package yet, apparently it is also not intended, see discussion in 
  https://gerrit.cloudera.org/#/c/8920/)
- investigate if we can make the storage format compatible to impala's (so impala and hive can share the table)
  this would probably require hive to recognize STORED AS KUDU as a keyword. Haven't found out where to do that.

## Working Test case
```sql
DELETE JARS;
add jar hdfs:///user/cvaliente/lib/kudu-serde-2.6.13-all.jar;

USE cvaliente;
DROP TABLE IF EXISTS cvaliente.test_drop;
CREATE TABLE if not exists cvaliente.test_drop (
id INT,
name STRING
)
stored by 'KuduStorageHandler'
TBLPROPERTIES(
  'kudu.table_name' = 'cvaliente_test_drop',
  'kudu.master_addresses' = 'mgmt0.hadoop.trivago.com:7051,mgmt1.hadoop.trivago.com:7051,mgmt2.hadoop.trivago.com:7051',
  'kudu.key_columns' = 'id',  
  'kudu.partition_columns' = 'id',
  'kudu.buckets.for.id' = '4'
);

describe formatted test_drop;

insert into test_drop values (1, 'a'), (2, 'b'), (3, 'a');

```
## Create a an External table on existing Kudu Table

```sql
create external table cvaliente.kudutest
ROW FORMAT SERDE
  'HiveKuduSerDe'
stored by 'KuduStorageHandler'
TBLPROPERTIES(
  'kudu.table_name' = 'original_kudu_name',
  'kudu.master_addresses' = 'mgmt0.hadoop.trivago.com:7051,mgmt1.hadoop.trivago.com:7051,mgmt2.hadoop.trivago.com:7051'
);
```