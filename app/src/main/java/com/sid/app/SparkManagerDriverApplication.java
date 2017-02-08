package com.sid.app;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.ChildFirstURLClassLoader;
import org.apache.spark.util.MutableURLClassLoader;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.ContextHandler;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.jetty.servlet.ServletHolder;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.util.Arrays;

/**
 * Created by Sid on 28-01-2017.
 */
public class SparkManagerDriverApplication {

    public static final Server SERVER = new Server(0);

    public static JavaSparkContext SPARK_CONTEXT;

    public static MutableURLClassLoader classLoader;

    public static void main(String[] args) throws IOException {

        System.out.println("Starting application. "+ Arrays.asList(args));

        //JavaSparkContext
        SPARK_CONTEXT = createNewContext(args);

        setClassLoader(Boolean.valueOf(SPARK_CONTEXT.getConf().get("spark.driver.userClassPathFirst", "false")));

        startServer(args);

    }



    protected static MutableURLClassLoader setClassLoader(boolean isChildFirst)
    {
        MutableURLClassLoader cl;
        if (isChildFirst) {
            cl=new ChildFirstURLClassLoader(new URL[]{}, Thread.currentThread().getContextClassLoader());
        } else {
            cl=new MutableURLClassLoader(new URL[]{}, Thread.currentThread().getContextClassLoader());
        }
        Thread.currentThread().setContextClassLoader(cl);
        classLoader = cl;
        System.out.println("classloader set as "+classLoader.getClass().getCanonicalName());
        return cl;
    }

    protected static void startServer(String[] args) throws IOException {
        Socket socket = new Socket();
        try
        {
            socket.connect(new InetSocketAddress(args[0], Integer.valueOf(args[1])));
        }
        catch (Exception e)
        {
            System.out.println("Could not connect. Exiting.");
            return;
        }
        if(!socket.isConnected() && !socket.getInetAddress().isReachable(100))
        {
            System.out.println("Could not connect. Exiting.");
            return;
        }
        attachOnInputStream(socket.getInputStream());
        OutputStream os = socket.getOutputStream();
        os.write(InetAddress.getLocalHost().getCanonicalHostName().getBytes());
        os.write(System.lineSeparator().getBytes());

        PackagesResourceConfig config = new PackagesResourceConfig("com/sid/app/resource");

        ServletHolder servlet = new ServletHolder(new ServletContainer(config));

        ContextHandler contextHandler = new ContextHandler();
        contextHandler.setContextPath("/api");

        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(servlet, "/*");
        contextHandler.setHandler(handler);

        SERVER.addHandler(contextHandler);

        try {
            SERVER.start();
            int localPort = SERVER.getConnectors()[0].getLocalPort();
            System.out.println("Started at port "+ localPort);
            os.write(String.valueOf(localPort).getBytes());
            os.write(System.lineSeparator().getBytes());
            SERVER.join();
        } catch (Exception e) {
            e.printStackTrace();
            try {
                SERVER.stop();
                return;
            } catch (Exception e1) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void attachOnInputStream(InputStream inputStream) {
        Thread serverWatcher = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    System.out.println("Attaching into input stream");
                    while(inputStream.read()!=-1)
                    {
                    }
                    System.out.println("Detected owner close. Shutting down server.");
                    SERVER.stop();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        serverWatcher.start();
    }

    private static JavaSparkContext createNewContext(String[] args) {
        SparkConf conf = new SparkConf();
        int length = 2;
        while(length<args.length)
        {
            conf.set(args[length], args[length+1]);
            length=length+2;
        }
        System.out.println("Temp dir : "+conf.get("spark.local.dir", "null"));
        Tuple2<String, String>[] all = conf.getAll();
        for(Tuple2<String, String> attr : all)
        {
            System.out.println("SparkConf attribute : "+attr._1()+"="+attr._2());
        }
        JavaSparkContext sc = new JavaSparkContext("yarn-cluster", "SparkManagerDriverApplication", conf);
        System.out.println("sc created " + sc);
        return sc;
    }
}
