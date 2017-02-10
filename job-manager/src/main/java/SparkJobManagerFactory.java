import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * Created by Sid on 08-02-2017.
 */
public abstract class SparkJobManagerFactory {

    public static SparkJobManagerFactory yarn(YarnConfiguration config, String appJar)
    {
        return new YarnSparkJobManagerFactory(config, appJar);
    }

    public abstract SparkJobManager build();

    public static class YarnSparkJobManagerFactory extends SparkJobManagerFactory
    {

        private final YarnConfiguration config;
        private final String appJar;

        public YarnSparkJobManagerFactory(YarnConfiguration config, String appJar) {
            this.config = config;
            this.appJar = appJar;
        }

        @Override
        public YarnSparkJobManager build() {
            return new YarnSparkJobManager(config, appJar);
        }
    }

}
