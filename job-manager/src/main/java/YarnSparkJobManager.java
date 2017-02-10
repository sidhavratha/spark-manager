import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * Created by Sid on 08-02-2017.
 */
public class YarnSparkJobManager implements SparkJobManager {

    private YarnConfiguration yarnConfiguration;
    private String appJar;

    public YarnSparkJobManager(YarnConfiguration config, String appJar)
    {
        this.yarnConfiguration = config;
        this.appJar = appJar;
    }

    @Override
    public SparkContextHandle createContext(String contextName, Map<String,String> sparkConfMap)
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
                    contextName,

                    "--driver-memory",
                    "100M",

                    "--jar",
                    this.appJar,

                    "--class",
                    "com.sid.app.SparkManagerDriverApplication",

                    "--arg",
                    InetAddress.getLocalHost().getHostAddress(),

                    "--arg",
                    String.valueOf(sc.getLocalPort())
            };
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        Client client = new Client(new ClientArguments(sparkArgs, sparkConf), yarnConfiguration, sparkConf);
        client.run();
        YarnSparkContextHandle yarnSparkContextHandle = detectServer(sc);
        return yarnSparkContextHandle;
    }

    private static YarnSparkContextHandle detectServer(ServerSocket serverSocket) {
        Socket sc;
        try {
            sc = serverSocket.accept();
            serverSocket.close();
            BufferedReader br = new BufferedReader(new InputStreamReader(sc.getInputStream()));
            String hostname = br.readLine();
            int port = Integer.valueOf(br.readLine());
            System.out.println("At server received : "+hostname+":"+port);
            return new YarnSparkContextHandle(hostname, port, sc);

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

}
