package com.offcn.bigdatawq.vehicle.jobs;
import com.offcn.bigdatawq.vehicle.entity.VehicleLog;
//import com.sun.tools.classfile.Synthetic_attribute;
import kafka.serializer.StringDecoder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RealTimeVehicleStatJob {
    public static void main(String[] args) throws Exception {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN);
        Logger.getLogger("org.spark_project").setLevel(Level.WARN);

        SparkConf conf = new SparkConf()
                .setAppName("RealTimeVehicleStatJob")
                .setMaster("local[*]");

        Duration batchInterval = Durations.seconds(2);
        JavaStreamingContext jsc = new JavaStreamingContext(conf, batchInterval);

        JavaPairInputDStream<String, String> messages = createKafkaMsg(jsc);

        // messages.print();

        messages.foreachRDD((JavaPairRDD<String, String> rdd, Time time) -> {
            if(!rdd.isEmpty()) {//rdd中有数据


                System.out.println("-------------------------------------------");
                System.out.println("Time: " + time);
                System.out.println("-------------------------------------------");
                processRDD(rdd);
            }
        });

        jsc.start();
        jsc.awaitTermination();

    } private static void processRDD(JavaPairRDD<String, String> rdd) {
        JavaRDD<VehicleLog> vehicleRDD = rdd.map((Tuple2<String, String> kv) -> {
            return VehicleLog.json2Bean(kv._2);
        }).filter(log -> log != null);
        vehicleRDD.foreach(logs -> System.out.println(logs));
        calcTagert(vehicleRDD);
    }
    private static void calcTagert(JavaRDD<VehicleLog> vehicleRDD) {

    }
    private static JavaPairInputDStream<String, String> createKafkaMsg(JavaStreamingContext jsc){
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "192.168.171.138:9092");
        kafkaParams.put("group.id", "sdlg-group");

        kafkaParams.put("auto.offset.reset", "smallest");

        Set<String> topics = new HashSet<>();
        topics.add("test");
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jsc,
                String.class, String.class,
                StringDecoder.class, StringDecoder.class,
                kafkaParams, topics);
        return messages;
    }
}

