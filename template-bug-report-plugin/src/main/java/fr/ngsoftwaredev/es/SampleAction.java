package fr.ngsoftwaredev.es;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.*;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequestBuilder;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

public final class SampleAction extends BaseRestHandler {

    public SampleAction(final Settings settings, final RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.GET, "_sample", this);
    }

    @Override
    public String getName() {
        return SampleAction.class.getSimpleName();
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        return channel -> {
            try {
                channel.sendResponse(new BytesRestResponse(
                        RestStatus.OK, "application/json", sample(client)));
            } catch (final Exception e) {
                channel.sendResponse(new BytesRestResponse(channel, e));
            }
        };
    }

    private String sample(final Client client) {

        // Store a search template (match_all)
        final PutStoredScriptResponse resp = client.admin().cluster().preparePutStoredScript()
                .setId("sample")
                .setContent(new BytesArray("{ \"query\": { \"match_all\": {}}}"
                        .getBytes(StandardCharsets.UTF_8)), XContentType.JSON)
                .get();
        assert resp.isAcknowledged();

        // Search with the template
        return new SearchTemplateRequestBuilder(client)
                .setScript("sample")
                .setScriptType(ScriptType.STORED)
                .setScriptParams(Collections.emptyMap())
                .setRequest(new SearchRequest())
                .get()
                .getResponse()
                .toString();
    }
}
