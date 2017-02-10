import java.util.Map;

/**
 * Created by Sid on 10-02-2017.
 */
public interface SparkJobManager {
    SparkContextHandle createContext(String contextName, Map<String,String> sparkConfMap);
}
