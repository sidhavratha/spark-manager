package com.sid.app;

import com.sun.jersey.api.uri.UriBuilderImpl;
import org.junit.Test;

import javax.ws.rs.core.UriBuilder;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by Sid on 08-02-2017.
 */
public class SparkManagerDriverApplicationTest {

    @Test
    public void testApp() throws IOException {
        final ServerSocket serverSocket = new ServerSocket(0);
        Thread listener = new Thread(new Runnable() {
            @Override
            public void run() {
                Socket sc=null;
                try {
                    sc = serverSocket.accept();
                    serverSocket.close();
                    BufferedReader br = new BufferedReader(new InputStreamReader(sc.getInputStream()));
                    String hostname = br.readLine();
                    int port = Integer.valueOf(br.readLine());
                    System.out.println("At server received : "+hostname+":"+port);

                    UriBuilder builder = new UriBuilderImpl();
                    builder.scheme("http").host(hostname).port(port).path("api").path("app").path("stop");


                    com.sun.jersey.api.client.Client client = new com.sun.jersey.api.client.Client();
                    String response = client.resource(builder.build()).get(String.class);
                    System.out.println("Stop response is "+response);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                finally {
                    try {
                        sc.close();
                        System.out.println("Closed socket at server");
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
        listener.start();
        SparkManagerDriverApplication.startServer(new String[]{"localhost", String.valueOf(serverSocket.getLocalPort())});
        SparkManagerDriverApplication.startServer(new String[]{"localhost", String.valueOf(serverSocket.getLocalPort())});
    }

}