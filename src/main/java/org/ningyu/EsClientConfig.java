package org.ningyu;

/* Config for EsClientFactory*/

import io.searchbox.client.config.HttpClientConfig;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Data
public class EsClientConfig {
    private boolean isAuth;
    private String userName;
    private String password;
    // url: http://192.168.1.1:9200
    private List<String> esUrls;
    // to discover other nodes of cluster
    private Boolean discoveryEnable;
    // discover other nodes frequency; seconds
    private Long discoveryFrequency;
    // use multiple thread
    private Boolean multiThreaded;
    // max idle time of connections; seconds
    private Integer maxConnectionIdleTime;
    // default max total connection per route
    private Integer defaultMaxTotalConnectionPerRoute;
    // max total connection
    private Integer maxTotalConnection;
    // connection time out
    private Integer connTimeout;
    // read time out
    private Integer readTimeout;

    /*
    * Constructor with all config default value.
    * */
    public EsClientConfig(boolean isAuth, String userName, String password, List<String> esUrls) {
        this.isAuth = isAuth;
        this.userName = userName;
        this.password = password;
        this.esUrls = esUrls;
        this.discoveryEnable = true;
        this.discoveryFrequency = 2L;
        this.multiThreaded = true;
        this.maxConnectionIdleTime = 360;
        this.defaultMaxTotalConnectionPerRoute = 30;
        this.maxTotalConnection = 60;
        this.connTimeout = 10000;
        this.readTimeout = 600000;
    }

    public EsClientConfig() {
    }

    public HttpClientConfig toHttpClientConfig() {
        HttpClientConfig.Builder builder = new HttpClientConfig.Builder(esUrls).
                discoveryEnabled(discoveryEnable).
                discoveryFrequency(discoveryFrequency, TimeUnit.SECONDS).
                multiThreaded(multiThreaded).
                maxConnectionIdleTime(360, TimeUnit.SECONDS).
                defaultMaxTotalConnectionPerRoute(defaultMaxTotalConnectionPerRoute).
                maxTotalConnection(maxTotalConnection).
                connTimeout(connTimeout).
                readTimeout(readTimeout);
        if (isAuth) {
            builder.defaultCredentials(userName, password);
        }
        return builder.build();
    }
}
