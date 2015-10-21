package org.elasticsearch.dbloader;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.*;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Ivan Zarudnyi on 11.10.2015.
 */
public class DataManager {
    private String connString, user, pass;

    public DataManager(String connString, String user, String pass) {
        this.connString = connString;
        this.user = user;
        this.pass = pass;
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(connString, user, pass);

    }

    public void loadData(BulkRequest bulkRequest) {
        Connection conn = null;

        try {
            conn = getConnection();

            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM wghlm_akf_members limit 100");
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            XContentBuilder jsonBuilder;


            while (rs.next()) {

                String label;
                String data;

                IndexRequest indexRequest = new IndexRequest("members", "member", "" + rs.getInt(1));

                jsonBuilder = jsonBuilder().startObject();
                for (int i = 1; i < columnCount; i++) {
                    label = metaData.getColumnLabel(i);
                    data = rs.getString(i);
                    jsonBuilder.field(label,data);

                }
                jsonBuilder.startArray("groups");
                jsonBuilder.startObject();
                jsonBuilder.field("test","test2");
                jsonBuilder.endObject();
                jsonBuilder.endArray();


                indexRequest.source(jsonBuilder);
                bulkRequest.add(indexRequest);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

}
