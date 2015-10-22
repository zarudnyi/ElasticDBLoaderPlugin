package org.elasticsearch.dbloader;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by root on 22.10.15.
 */
public class ConfigProvider {
    private static ConfigProvider instance;
    private Properties prop;

    public static ConfigProvider getInstance(){
        return instance;
    }

    private ConfigProvider(String path) throws IOException {
        prop = new Properties();
        FileInputStream fis = new FileInputStream(path);
        prop.loadFromXML(fis);
    }

    public static void init () throws IOException {
        instance = new ConfigProvider("/etc/elasticsearch/dbloader.xml");
    }

    public String getHost (){
        return prop.getProperty("connection.host");
    }
    public String getPort(){
        return prop.getProperty("connection.port");
    }
    public String getPassword(){
        return prop.getProperty("connection.password");
    }

    public String getUser(){
        return prop.getProperty("connection.user");
    }

    public String getQuery(String index){
        return prop.getProperty(index+".query");
    }

    public String getQueryWhere(String index) {
        return prop.getProperty(index+".querywhere");
    }
}
