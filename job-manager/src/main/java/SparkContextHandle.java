import java.io.InputStream;
import java.util.Map;

/**
 * Created by Sid on 10-02-2017.
 */
public interface SparkContextHandle {
    void stopApplication();
    String submitApp(String appname, Map<String, String> params);
    String addJar(InputStream is);
}
