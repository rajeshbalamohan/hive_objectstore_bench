/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Deadline;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is for quickly benchmarking whether Metastore has been running fine or not, in
 * multi-threaded scenario.
 * <p>
 * <p>
 * E.g:
 * java -cp /etc/hive/conf/conf.server/:/usr/hdp/2.5.3.0-37/hive/lib/*:`hadoop classpath`:.: -Dconcurrency=6  ObjectStoreBench tpcds_orc INFO catalog_returns
 * <p>
 * For getting JDBC log files, you might have to change the URL or the INFO log level
 * e.g
 * 1. For MS-SQL server, just specify "INFO" in the log level as the example given in the earlier
 * doc.
 * 2. For mysql, -Djavax.jdo.option.ConnectionURL="jdbc:mysql://localhost/hive?createDatabaseIfNotExist=true&logger=com.mysql.jdbc.log.Slf4JLogger&profileSQL=true"
 */
public class ObjectStoreBench {

  final static List<Long> objStoreInitResponseTime =
      Collections.synchronizedList(new LinkedList<Long>());
  final static List<Long> getTablesResponseTime =
      Collections.synchronizedList(new LinkedList<Long>());
  final static Map<String, List<Long>> getTableResponseTime = new ConcurrentHashMap<>();
  final static Map<String, List<Long>> getPartitionsResponseTime = new ConcurrentHashMap<>();

  public static final String CONNECTION_URL = "javax.jdo.option.ConnectionURL";

  static class Worker implements Callable {

    String fetchTable;
    String databaseName;

    public Worker(String databaseName, String fetchTable) {
      this.fetchTable = fetchTable;
      this.databaseName = databaseName;
    }

    @Override
    public Object call() throws Exception {
      HiveConf conf = new HiveConf();
      System.out.println("javax.jdo.option.ConnectionURL: "
          + conf.get(CONNECTION_URL));

      //Check if this is being overridden
      String url = System.getProperty(CONNECTION_URL, null);
      if (url != null) {
        conf.set(CONNECTION_URL, url);
        System.out.println("Overriding with : " + conf.get(CONNECTION_URL));
      }

      long socketTimeout = HiveConf
          .getTimeVar(conf, HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT,
              TimeUnit.MILLISECONDS);

      ObjectStore objStore = new ObjectStore();
      try {
        Deadline.registerIfNot(socketTimeout);
        boolean isStarted = Deadline.startTimer("getPartitions");

        //Create object store
        long sTime = System.currentTimeMillis();
        objStore.setConf(conf);
        long eTime = System.currentTimeMillis();

        objStoreInitResponseTime.add(eTime - sTime);

        sTime = System.currentTimeMillis();
        List<String> tables = objStore.getAllTables(databaseName);
        eTime = System.currentTimeMillis();
        getTablesResponseTime.add(eTime - sTime);

        for (String tableName : tables) {
          if (!fetchTable.equalsIgnoreCase("ALL")) {
            if (!tableName.equals(fetchTable)) {
              continue;
            }
          }

          //For every table try to get partitions (ignore the ones without partitions)
          sTime = System.currentTimeMillis();
          Table table = objStore.getTable(databaseName, tableName);
          eTime = System.currentTimeMillis();

          synchronized (getTableResponseTime) {
            List<Long> responseTimes = getTableResponseTime.get(tableName);
            if (responseTimes == null) {
              responseTimes = new LinkedList<>();
              getTableResponseTime.put(tableName, responseTimes);
            }
            responseTimes.add(eTime - sTime);
          }

          if (table.getPartitionKeys() != null && !table.getPartitionKeys().isEmpty()) {
            sTime = System.currentTimeMillis();
            List<Partition> partitionList = objStore.getPartitions(databaseName, tableName, -1);
            eTime = System.currentTimeMillis();

            synchronized (getPartitionsResponseTime) {
              List<Long> responseTimes = getPartitionsResponseTime.get(tableName);
              if (responseTimes == null) {
                responseTimes = new LinkedList<>();
                getPartitionsResponseTime.put(tableName, responseTimes);
              }
              responseTimes.add(eTime - sTime);
            }

            log("Partitions=" + partitionList.size() + ", table=" + tableName);
          }
        }
        objStore.shutdown();
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        log("Cleaning up...");
        objStore.shutdown();
      }

      return null;
    }
  }

  public static void log(String msg) {
    System.out.println("Thread: " + Thread.currentThread().getId() + ", msg: " + msg);
  }

  static double getAvg(List<Long> list) {
    long total = 0;
    for (Long time : list) {
      total += time;
    }
    return (total * 1.0d) / (list.size() * 1.0d);
  }

  /**
   * Print response times
   */
  public static void printResponseTimes() {
    System.out.println("Avg object store time (ms) : " + getAvg(objStoreInitResponseTime));
    System.out.println("Avg getAllTables (ms) : " + getAvg(getTablesResponseTime));

    for (Map.Entry<String, List<Long>> entry : getTableResponseTime.entrySet()) {
      String tableName = entry.getKey();
      System.out.println("TableName: " + tableName
          + ", avg response time (ms) : " + getAvg(entry.getValue()));
    }

    for (Map.Entry<String, List<Long>> entry : getPartitionsResponseTime.entrySet()) {
      String tableName = entry.getKey();
      System.out.println("getPartitions for TableName: " + tableName
          + ", avg response time (ms) : " + getAvg(entry.getValue()));
    }
  }

  public static void main(String[] args)
      throws InterruptedException, ExecutionException {

    Integer count = Integer.parseInt(System.getProperty("concurrency", "1"));
    System.out.println("Number of threads : " + count);

    //TODO : Don't bother about options
    String databaseName = args[0];
    if (databaseName == null || databaseName.isEmpty()) {
      System.out.println("Please provide valid DB name");
      System.exit(-1);
    }
    System.out.println("DATABASE NAME : " + databaseName);

    Level logLevel = Level.INFO;
    //TODO : Don't bother about options
    if (args.length >= 2) {
      logLevel = Level.parse(args[1]);
    }
    System.out.println("Log level used : " + logLevel);

    //All tables?
    String fetchTable = "ALL";
    if (args.length >= 3) {
      fetchTable = args[2];
    }

    //TODO : This is for MS-SQL server metastore
    ConsoleHandler consoleHandler = new ConsoleHandler();
    consoleHandler.setLevel(Level.ALL);

    Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc");
    logger.addHandler(consoleHandler);
    logger.setLevel(logLevel);

    //TOOD: impl tp
    ExecutorService tp = Executors.newFixedThreadPool(count);
    List<Future> futureList = new LinkedList<>();
    for (int i = 0; i < count; i++) {
      futureList.add(tp.submit(new Worker(databaseName, fetchTable)));
    }
    try {
      for (Future f : futureList) {
        f.get();
      }
    } finally {
      tp.shutdown();
    }
    printResponseTimes();
    System.out.println("Done!!!");
  }
}
