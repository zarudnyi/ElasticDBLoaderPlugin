/**
 * Created by Ivan Zarudnyi on 10.10.2015.
 */
package org.elasticsearch.dbloader;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.elasticsearch.rest.RestRequest.Method.*;

public class RestHandler extends BaseRestHandler {
    @Inject
    public RestHandler(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(POST, "/_dbloader", this);
        controller.registerHandler(PUT, "/_dbloader", this);
    }

    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        final BulkRequest bulkRequest = Requests.bulkRequest();
        JSONObject params =new JSONObject( new String(request.content().array()));


        String index = params.getString("index");
        String type = params.getString("type");
        JSONArray ids = params.getJSONArray("ids");


        channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR,params.toString()));


        DataLoader dl = new DataLoader(ConfigProvider.getInstance());

        try {
            if (request.method().equals(POST))
                dl.updateData(bulkRequest, index, type, ids);
            else
                dl.indexData(bulkRequest,index,type,ids);
        } catch (Exception e) {
            channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.toString()));
        }


        client.bulk(bulkRequest, new RestBuilderListener<BulkResponse>(channel) {
            @Override
            public RestResponse buildResponse(BulkResponse response, XContentBuilder builder) throws Exception {
                return new BytesRestResponse( RestStatus.OK,""+ (response.hasFailures()?response.buildFailureMessage():"OK"));
            }
        });
    }
}
