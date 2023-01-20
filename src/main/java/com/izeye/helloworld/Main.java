package com.izeye.helloworld;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.TransportUtils;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Hello world for Elasticsearch Java Client.
 *
 * @author Johnny Lim
 */
public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        String hostname = "localhost";
        int port = 9200;

        String fingerprint = "85a7b0c2ff6bbcbf77c4e0761c6b15342bafb28b861b5e806cc026ec90c59c87";

        String username = "elastic";
        String password = "viBqnt6oVD-scLvv_QKZ";

        SSLContext sslContext = TransportUtils.sslContextFromCaFingerprint(fingerprint);
        BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
        basicCredentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClient restClient = RestClient.builder(new HttpHost(hostname, port, "https"))
                .setHttpClientConfigCallback(hc -> hc.setSSLContext(sslContext).setDefaultCredentialsProvider(basicCredentialsProvider))
                .build();
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        ElasticsearchClient client = new ElasticsearchClient(transport);

        Product product = new Product("bk-1", "City bike", 123.0);
        IndexResponse response = client.index(i -> i.index("products").id(product.sku()).document(product));
        System.out.println("Indexed with version " + response.version());

        // Wait for indexing.
        TimeUnit.SECONDS.sleep(1);

        SearchResponse<Product> search = client.search(s -> s.index("products").query(q -> q.term(t -> t.field("name.keyword").value(v -> v.stringValue("City bike")))), Product.class);
        for (Hit<Product> hit : search.hits().hits()) {
            System.out.println(hit.source());
        }

        restClient.close();
    }

    record Product(String sku, String name, double price) {
    }
}
