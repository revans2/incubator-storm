package org.apache.storm.metrics2.store;

import java.util.HashMap;

class RocksDBBench {
    Double sum;
    public RocksDBBench(){
        sum = 0.0;
    }

    public static void main(String[] args){
        RocksDBBench bench = new RocksDBBench();
        RocksDBStore conn = new RocksDBStore();

        HashMap<String, Object> conf = new HashMap<>();
        conf.put("storm.metrics2.store.rocksdb.create_if_missing", true);
        conf.put("storm.metrics2.store.rocksdb.location", "/tmp/rocks_bench");
        //conf.put("storm.metrics2.store.rocksdb.optimize_filters_for_hits", false);
        conf.put("storm.metrics2.store.rocksdb.optimize_level_style_compaction", true);
        conf.put("storm.metrics2.store.rocksdb.table_type", "block");
        //conf.put("storm.metrics2.store.rocksdb.optimize_level_style_compaction_memtable_memory_budget_mb", 256);

        //conf.put("storm.metrics2.store.rocksdb.total_threads", 4);
        conn.prepare(conf);

        String topo = "topo1";

        int numMetricsPerExecutor = 10;
        int numExecutors = 20;
        int numDays = 1;

        System.out.println ("Insert test");
        long startTime = System.currentTimeMillis();
        Double value = 0.0;
        for (int e = 0; e < numExecutors; e++){
            for (int num = 0; num < numMetricsPerExecutor; num++){
                String metric = "metric" + num;
                
                int samplesADay = 1*60*24;
                int numSamples = samplesADay * numDays;
                for (long j = 0; j < numSamples; j++){
                    Metric m = new Metric(metric, j, Integer.toString(e), "comp1", "default", topo, value);
                    conn.insert(m);
                    value++;
                }
            }
        }

        long time = System.currentTimeMillis() - startTime;
        System.out.println("Wrote " + value + " rows in " + time + " ms");
        System.out.println(conn.getStats());

        long runningSum = 0;
        int numIter = 5;
        for (int i = 0; i < numIter; i++){
            startTime = System.currentTimeMillis();
            System.out.println("Full scan test " + i);
            conn.scan((metric, timeRanges) -> bench.sum += metric.getValue());
            time = System.currentTimeMillis() - startTime;
            System.out.println("SUM: " + bench.sum + " in " + time + " ms");
            System.out.println(conn.getStats());
            runningSum += time;
        }

        System.out.println ("AVG: " + runningSum/numIter);

        startTime = System.currentTimeMillis();
        System.out.println("Full remove test");
        conn.remove(new HashMap<String, Object>());
        time = System.currentTimeMillis() - startTime;
        System.out.println("Deleted in " + time + " ms");
        System.out.println(conn.getStats());
    }
}
