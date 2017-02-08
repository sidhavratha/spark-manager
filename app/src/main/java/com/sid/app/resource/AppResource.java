package com.sid.app.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sid.app.SparkManagerDriverApplication;
import com.sid.client.api.ClientApplication;
import org.apache.commons.io.FileUtils;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Created by Sid on 08-02-2017.
 */
@Path("/app")
public class AppResource {

    @Path("/name")
    @GET
    public String getName()
    {
        return "spark-job-manager";
    }

    @Path("/stop")
    @GET
    public String stop() throws Exception {
        System.out.println("Stopping server on request.");
        Thread stopThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    System.out.println("Stopping server in 2 secs");
                    Thread.sleep(TimeUnit.SECONDS.toMillis(2));
                    SparkManagerDriverApplication.SERVER.stop();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        stopThread.start();
        return Boolean.TRUE.toString();

    }

    @Path("/submit/{appClassName}")
    @GET
    public String submitJob(@PathParam("appClassName") String appClassName, @Context UriInfo uriInfo) throws Exception {
        Thread.currentThread().setContextClassLoader(SparkManagerDriverApplication.classLoader);
        System.out.println("Request received to submit job");
        String jobId = UUID.randomUUID().toString();
        SparkManagerDriverApplication.SPARK_CONTEXT.setJobGroup(jobId, jobId);
        HashMap<String, String> parameters = new HashMap<>();
        MultivaluedMap<String, String> queryParameters = uriInfo.getQueryParameters();
        for(Map.Entry<String, List<String>> entry : queryParameters.entrySet())
        {
            parameters.put(entry.getKey(), entry.getValue().get(0));
        }
        Serializable response = ((ClientApplication)(Class.forName(appClassName, true, SparkManagerDriverApplication.classLoader).newInstance())).execute(SparkManagerDriverApplication.SPARK_CONTEXT, parameters);
        return new ObjectMapper().writeValueAsString(response);

    }

    @Path("/jars/add")
    @POST
    public String addJar(InputStream jarStream) throws Exception {
        System.out.println("Request received to add jar");
        URL url = copyStreamToUrl(jarStream);
        SparkManagerDriverApplication.classLoader.addURL(url);
        String dst = new org.apache.hadoop.fs.Path(url.getPath()).getName();//new org.apache.hadoop.fs.Path(url.getFile());
        File file = new File(dst);
        if(!file.exists())
        {
            System.out.println("Copying to "+dst);
            FileUtils.copyFile(new File(url.getFile()), file);
            System.out.println("After copy "+file.exists());
        }
//        FileUtil.copy(new File(url.toString()), FileSystem.get(SparkManagerDriverApplication.SPARK_CONTEXT.hadoopConfiguration()), dst, false, SparkManagerDriverApplication.SPARK_CONTEXT.hadoopConfiguration());
        System.out.println("Calling spark context addjar for uri "+url.toString());
        SparkManagerDriverApplication.SPARK_CONTEXT.addJar(url.toString());
        return Boolean.TRUE.toString();

    }

    private URL copyStreamToUrl(InputStream jarStream) {
        try {
            File client_jar = File.createTempFile("client_jar", UUID.randomUUID().toString() + ".jar");
            FileUtils.copyInputStreamToFile(jarStream, client_jar);
            System.out.println("File created at "+client_jar.toURI().toURL());
            return client_jar.toURI().toURL();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
