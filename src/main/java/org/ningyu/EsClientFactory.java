package org.ningyu;

import io.searchbox.client.JestClientFactory;

public class EsClientFactory {
    public static EsClient getClient(EsClientConfig config) {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(config.toHttpClientConfig());
        return new EsClient(factory.getObject());
    }
}
