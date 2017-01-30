Very basic test to check if MetaStore has perf issues due to backend DB.

It issues some basic commands in ObjectStore to get the response times. Examples are

1. Instantiate ObjectStore for every thread
2. getAllTables in a DB
3. getTable for every hive table
4. getPartitions() for the table which has partition info.

So basically, certain basic operations are tested here.

Just run "mvn clean package" and here are few examples

Spins up 6 threads
===================
java -cp /etc/hive/conf/conf.server/:/usr/hdp/2.5.3.0-37/hive/lib/*:`hadoop classpath`:.:
-Dconcurrency=6 -Djavax.jdo.option
.ConnectionURL="jdbc:mysql://localhost/hive?createDatabaseIfNotExist=true"  ObjectStoreBench tpcds_orc INFO catalog_returns

Printing JDBC driver logs:
==========================
java -cp /etc/hive/conf/conf.server/:/usr/hdp/2.5.3.0-37/hive/lib/*:`hadoop classpath`:.:
-Dconcurrency=6 -Djavax.jdo.option
.ConnectionURL="jdbc:mysql://localhost/hive?createDatabaseIfNotExist
=true&logger=com.mysql.jdbc.log.Slf4JLogger&profileSQL=true"  ObjectStoreBench tpcds_orc INFO catalog_returns

For MS-SQL server, ConsoleHelper is already embedded in program. So you can just provide "INFO"
logging level in the command line (other options include FINE, FINER, FINEST)