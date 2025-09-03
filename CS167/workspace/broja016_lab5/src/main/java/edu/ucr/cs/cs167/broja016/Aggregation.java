package edu.ucr.cs.cs167.broja016;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.io.IOException;
import java.util.Map;

public class Aggregation {
    public static void main(String[] args) throws IOException {
        final String inputPath = args[0];
        SparkConf conf = new SparkConf();
        if (!conf.contains("spark.master"))
            conf.setMaster("local[*]");
        System.out.printf("Using Spark master '%s'\n", conf.get("spark.master"));
        conf.setAppName("CS167-Lab5");
        try (JavaSparkContext spark = new JavaSparkContext(conf)) {
            JavaRDD<String> logFile = spark.textFile(inputPath);
            // To do 1: transform via `mapToPair`, return `Tuple2`
            JavaPairRDD<String, Integer> codes = logFile
                    .mapToPair(line -> {
                        String[] parts = line.split("\t");
                        String responseCode = parts[5];
                        return new Tuple2<>(responseCode,1);
                    });
            // To do 2: `countByKey`
            Map<String, Long> counts = codes.countByKey();

            for (Map.Entry<String, Long> entry : counts.entrySet()) {
                System.out.printf("Code '%s' : number of entries %d\n", entry.getKey(), entry.getValue());
            }
            System.in.read(); // the program will keep running until you input something
        }
    }
}
