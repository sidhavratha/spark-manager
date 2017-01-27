import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Sid on 26-01-2017.
 */
public class YarnClusterTest {
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
    }
}
