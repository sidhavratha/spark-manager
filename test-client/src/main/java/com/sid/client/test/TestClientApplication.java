package com.sid.client.test;

import com.sid.client.api.ClientApplication;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

/**
 * Created by Sid on 08-02-2017.
 */
public class TestClientApplication implements ClientApplication {

    @Override
    public Serializable execute(JavaSparkContext context, Map<String, String> parameters) {
        String values = parameters.get("values");
        String[] splits = values.split(",");
        JavaRDD<String> rdd = context.parallelize(Arrays.asList(splits));
        System.out.println("RDD is : "+rdd.collect());
        int sum = rdd.map(v -> Integer.parseInt(v)).reduce((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);
        System.out.println("Sum is : "+ sum);
        return sum;
    }
}
