package com.sid.client.api;

import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by Sid on 08-02-2017.
 */
public interface ClientApplication extends Serializable {
    Serializable execute(JavaSparkContext context, Map<String, String> parameters);
}
