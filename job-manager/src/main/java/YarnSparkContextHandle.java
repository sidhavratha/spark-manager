import com.sun.jersey.api.uri.UriBuilderImpl;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.Map;

/**
 * Created by Sid on 10-02-2017.
 */
public class YarnSparkContextHandle implements SparkContextHandle {


    private final UriBuilder builder;
    private final Socket appConnection;

    public YarnSparkContextHandle(String hostName, int port, Socket socket)
    {
        this.builder = new UriBuilderImpl().scheme("http").host(hostName).port(port).path("api");
        this.appConnection = socket;
    }

    @Override
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

    @Override
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

    @Override
    public String addJar(InputStream is) {
        com.sun.jersey.api.client.Client client = new com.sun.jersey.api.client.Client();
        String response = client.resource(builder.clone().path("app").path("jars").path("add").build()).entity(is).post(String.class);
        System.out.println("Add jar response is "+response);
        return response;
    }

}
