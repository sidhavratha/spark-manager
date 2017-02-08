import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.UnknownHostException;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

/**
 * Created by Sid on 26-01-2017.
 */
public class YarnClusterIT {

    private static MiniYARNCluster miniYarnCluster;

    @BeforeClass
    static public void setup() throws Exception {
        miniYarnCluster = MiniClusterUtil.createMiniYARNCluster(1);
        YarnConfiguration appConf = new YarnConfiguration(miniYarnCluster.getConfig());
        SparkJobManager.init(appConf, "../app/target/app-1.0-SNAPSHOT.jar");
    }

    @AfterClass
    static public void teardown() throws Exception {
        SparkJobManager.reset();
        miniYarnCluster.stop();
        miniYarnCluster.close();
    }

    @Test
    public void testMiniYarnCluster() throws UnknownHostException {

        HashMap<String, String> sparkConfMap = new HashMap<>();
        sparkConfMap.put("spark.testing.reservedMemory", "50000000");
        SparkJobManager jobManager = SparkJobManager.createContext("context1", sparkConfMap);
        try {
            jobManager.addJar(new FileInputStream("../test-client/target/test-client-1.0-SNAPSHOT.jar"));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        try
        {
            HashMap<String, String> params = new HashMap<>();
            params.put("values", "1,2,3,4,5");
            String response = jobManager.submitApp("com.sid.client.test.TestClientApplication", params);
            assertEquals("15", response);

            params.put("values", "1,2,3,4,5,6");
            response = jobManager.submitApp("com.sid.client.test.TestClientApplication", params);
            assertEquals("21", response);
        }
        finally {
            jobManager.stopApplication();
        }
        System.out.println("Completed");
    }
}
