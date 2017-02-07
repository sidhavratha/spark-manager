import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Sid on 26-01-2017.
 */
public class YarnClusterIT {

    private static MiniYARNCluster miniYarnCluster;

    @BeforeClass
    static public void setup() throws Exception {
        miniYarnCluster = MiniClusterUtil.createMiniYARNCluster(1);
    }

    @AfterClass
    static public void teardown() throws Exception {
        miniYarnCluster.stop();
        miniYarnCluster.close();
    }

    @Test
    public void testMiniYarnCluster()
    {

        assertNotNull(miniYarnCluster);

        YarnConfiguration appConf = new YarnConfiguration(miniYarnCluster.getConfig());
        assertNotNull(appConf);
        System.setProperty("SPARK_YARN_MODE", "true");

        SparkConf sparkConf = new SparkConf();

        String[] sparkArgs = {
                "--name",
                "MySparkApp",

                "--driver-memory",
                "100M",

                "--jar",
                "../test-app/target/test-app-1.0-SNAPSHOT.jar",

                "--class",
                "MySparkApp",

                "--arg",
                "spark.testing.reservedMemory",

                "--arg",
                "50000000"
        };
        Client client = new Client(new ClientArguments(sparkArgs, sparkConf), appConf, sparkConf);
        client.run();
        System.out.println("Completed");
    }

    /*
    @Test
    public void testMiniYarnCluster()
    {
        YarnConfiguration clusterConf = new YarnConfiguration();
        clusterConf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
        clusterConf.setClass(YarnConfiguration.RM_SCHEDULER,
                FifoScheduler.class, ResourceScheduler.class);
        MiniYARNCluster miniCluster = new MiniYARNCluster("MiniYarnCluster", 1, 1, 1);
        miniCluster.init(clusterConf);
        miniCluster.start();
        assertNotNull(miniCluster);
//once the cluster is created, you can get its configuration
//with the binding details to the cluster added from the minicluster
        YarnConfiguration appConf = new YarnConfiguration(miniCluster.getConfig());
        assertNotNull(appConf);
        System.setProperty("SPARK_YARN_MODE", "true");

        SparkConf sparkConf = new SparkConf();
//        JavaSparkContext sc = new JavaSparkContext("yarn-client", "YarnClusterTest", sparkConf);
//        assertNotNull(sc);

        String[] sparkArgs = {
                "--name",
                "MySparkApp",

                "--driver-memory",
                "100M",

                "--jar",
                "../test-app/target/test-app-1.0-SNAPSHOT.jar",

                "--class",
                "MySparkApp",

                "--arg",
                "spark.testing.reservedMemory",

                "--arg",
                "50000000"
        };
        Client client = new Client(new ClientArguments(sparkArgs, sparkConf), appConf, sparkConf);
        client.run();
        System.out.println("Completed");
    }*/
}
