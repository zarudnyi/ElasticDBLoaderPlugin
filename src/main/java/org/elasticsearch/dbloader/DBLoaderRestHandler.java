/**
 * Created by Ivan Zarudnyi on 10.10.2015.
 */
package org.elasticsearch.dbloader;

import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.*;
import static org.elasticsearch.rest.RestStatus.OK;

public class DBLoaderRestHandler extends BaseRestHandler {
    @Inject
    public DBLoaderRestHandler(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/_test", this);
        controller.registerHandler(PUT, "/_test", this);

    }

    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        BulkRequest bulkRequest = Requests.bulkRequest();

        DataManager dm = new DataManager("jdbc:mysql://localhost:3306/choo_api?zeroDateTimeBehavior=convertToNull","root","");

        dm.loadData(bulkRequest);
        System.out.println("=====CLIENT START====" + new Date());
        client.bulk(bulkRequest, new RestBuilderListener<BulkResponse>(channel) {
            @Override
            public RestResponse buildResponse(BulkResponse response, XContentBuilder builder) throws Exception {

                System.out.println("=====DONE====" + new Date());

                return new BytesRestResponse(OK, ""+client.getClass());
            }
        });
    }
}
