# HiveUDF
This Hive UDF sample code contains 2 functions: MyUpper() and MyContains()

## a. How to build the jar

```shell
mvn package
```

##b. Prepare a Hive table with sample data

In Hive CLI, create a test table:

```sql
CREATE TABLE testarray(col1 string, col2 string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "|";
CREATE TABLE testarray2(col1 array<string>, col2 string) ;
```

Put below data for testarray:

```shell
echo "abc|xyz" > test.data
```

Insert into testarray2:

```sql
insert into table testarray2 select array(col1,col2),col2 from testarray;

hive> select * from testarray2 ;
OK
["abc","xyz"]	xyz
Time taken: 0.06 seconds, Fetched: 1 row(s)
```

##c. Test UDF

```sql
ADD JAR ~/target/MyUDF-1.0.0.jar;
CREATE TEMPORARY FUNCTION MyUpper AS 'openkb.hive.udf.MyUpper'; 
CREATE TEMPORARY FUNCTION MyContains AS 'openkb.hive.udf.MyContains'; 

SELECT MyUpper(col2) FROM testarray2;
XYZ

SELECT MyContains(col1,col2) FROM testarray2;
true
```
