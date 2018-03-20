/**
 * Copyright 2018 Ambud Sharma
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.srotya.sidewinder.clients;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class QueryClient {

	public static void main(String[] args)
			throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException, InterruptedException {
		long ts = System.currentTimeMillis();
		ExecutorService es = Executors.newFixedThreadPool(8);
		CloseableHttpClient client = buildClient("http://localhost:8080/influx/query", 3000, 120_000);
		for (int i = 0; i < 10000; i++) {
			es.submit(() -> {
				try {
					HttpPost request = new HttpPost("http://localhost:8080/influx/query");
					request.setHeader("Content-Type", "application/json");
					request.setEntity(
							new StringEntity("{\"panelId\":1,\"range\":{\"from\":\"2017-10-25T04:40:31.840Z\","
									+ "\"to\":\"2017-10-25T05:47:53.477Z\",\"raw\":{\"from\":\"2017-10-25T04:40:31.840Z\",\"to\""
									+ ":\"2017-10-25T00:47:53.477Z\"}},\"rangeRaw\":{\"from\":\"2017-10-25T04:40:31.840Z\",\"to\":\"2017-10-25T05:47:53.477Z\"},"
									+ "\"interval\":\"2s\",\"intervalMs\":2000,\"targets\":[{\"target\":\"cpu\",\"filters\":[],\"aggregator\":"
									+ "{\"args\":[{\"index\":0,\"type\":\"int\",\"value\":\"1200\"}],\"name\":\"average\",\"unit\":\"secs\"},\"field\":\".*\",\"refId\":\"A\",\"type\":\"timeserie\"}],\"format\":\"json\",\"maxDataPoints\":1272}"));
					CloseableHttpResponse execute = client.execute(request);
					EntityUtils.consume(execute.getEntity());
					execute.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
		}
		es.shutdown();
		es.awaitTermination(1000, TimeUnit.SECONDS);
		ts = System.currentTimeMillis() - ts;
		System.out.println("1k queries completed in:" + ts + "ms");
	}

	public static CloseableHttpClient buildClient(String baseURL, int connectTimeout, int requestTimeout)
			throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
		HttpClientBuilder clientBuilder = HttpClients.custom();
		RequestConfig config = RequestConfig.custom().setConnectTimeout(connectTimeout)
				.setConnectionRequestTimeout(requestTimeout).build();

		return clientBuilder.setDefaultRequestConfig(config).build();
	}

}
