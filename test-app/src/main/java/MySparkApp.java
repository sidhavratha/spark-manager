import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by Sid on 28-01-2017.
 */
public class MySparkApp {
    public static void main(String[] args) throws IOException {
        //JavaSparkContext
        System.out.println("hello client");
//        throw new RuntimeException("Some exception");
//        new File("hello-world.txt").createNewFile();
        SparkConf conf = new SparkConf();
        int length = 0;
        while(length<args.length)
        {
            conf.set(args[length], args[length+1]);
            length=length+2;
        }
        JavaSparkContext sc = new JavaSparkContext("yarn-cluster", "YarnClusterTest", conf);
        System.out.println("sc created " + sc);
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        System.out.println("RDD is : "+rdd.collect());
        System.out.println("Sum is : "+rdd.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        }));
    }
}
