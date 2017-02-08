import com.sun.jersey.api.uri.UriBuilderImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;

import javax.ws.rs.core.UriBuilder;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Sid on 08-02-2017.
 */
public class SparkJobManager {

    private static YarnConfiguration yarnConfiguration;
    private static String appJar;

    private static Map<String, SparkJobManager> MANAGER_PER_CONTEXT = new HashMap<>();

    private final UriBuilder builder;
    private Socket appConnection;

    private SparkJobManager(String hostName, int port, Socket socket)
    {
        this.builder = new UriBuilderImpl().scheme("http").host(hostName).port(port).path("api");
        this.appConnection = socket;
    }

    public static void init(YarnConfiguration config, String appJar)
    {
        SparkJobManager.yarnConfiguration = config;
        SparkJobManager.appJar = appJar;
    }

    public static void reset()
    {
        SparkJobManager.yarnConfiguration = null;
        SparkJobManager.appJar = null;
    }

    public static SparkJobManager createContext(String contextName, Map<String,String> sparkConfMap)
    {
        System.setProperty("SPARK_YARN_MODE", "true");

        SparkConf sparkConf = new SparkConf();
        for(Map.Entry<String,String> entry : sparkConfMap.entrySet())
        {
            sparkConf.set(entry.getKey(), entry.getValue());
        }
        sparkConf.set("spark.yarn.submit.waitAppCompletion", Boolean.FALSE.toString());

        ServerSocket sc;
        try {
            sc = new ServerSocket(0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String[] sparkArgs;
        try {
            sparkArgs = new String[]{
                    "--name",
                    "com.sid.app.SparkManagerDriverApplication",

                    "--driver-memory",
                    "100M",

                    "--jar",
                    SparkJobManager.appJar,

                    "--class",
                    "com.sid.app.SparkManagerDriverApplication",

                    "--arg",
                    InetAddress.getLocalHost().getCanonicalHostName(),

                    "--arg",
                    String.valueOf(sc.getLocalPort())
            };
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        Client client = new Client(new ClientArguments(sparkArgs, sparkConf), yarnConfiguration, sparkConf);
        client.run();
        SparkJobManager sparkJobManager = detectServer(sc);
        MANAGER_PER_CONTEXT.put(contextName, sparkJobManager);
        return sparkJobManager;
    }



    private static SparkJobManager detectServer(ServerSocket serverSocket) {
        Socket sc;
        try {
            sc = serverSocket.accept();
            serverSocket.close();
            BufferedReader br = new BufferedReader(new InputStreamReader(sc.getInputStream()));
            String hostname = br.readLine();
            int port = Integer.valueOf(br.readLine());
            System.out.println("At server received : "+hostname+":"+port);
            return new SparkJobManager(hostname, port, sc);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            if(!serverSocket.isClosed())
            {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public void stopApplication() {
        com.sun.jersey.api.client.Client client = new com.sun.jersey.api.client.Client();
        try
        {
            String response = client.resource(builder.clone().path("app").path("stop").build()).get(String.class);
            System.out.println("Stop response is "+response);
        }
        catch (Exception e)
        {
            System.out.println("Server stopped.");
        }
        try {
            System.out.println("Closing socket at server "+this.appConnection.isClosed());
            if(!this.appConnection.isClosed())
            {
                this.appConnection.close();
                System.out.println("Closed socket at server");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String submitApp(String appname, Map<String, String> params) {
        com.sun.jersey.api.client.Client client = new com.sun.jersey.api.client.Client();
        UriBuilder path = builder.clone().path("app").path("submit").path(appname);
        for(Map.Entry<String, String> entry : params.entrySet())
        {
            path = path.queryParam(entry.getKey(), entry.getValue());
        }
        String response = client.resource(path.build()).get(String.class);
        System.out.println("Submit response is "+response);
        return response;
    }

    public String addJar(InputStream is) {
        com.sun.jersey.api.client.Client client = new com.sun.jersey.api.client.Client();
        String response = client.resource(builder.clone().path("app").path("jars").path("add").build()).entity(is).post(String.class);
        System.out.println("Add jar response is "+response);
        return response;
    }
}
