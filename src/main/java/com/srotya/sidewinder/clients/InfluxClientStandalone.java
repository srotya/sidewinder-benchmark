/**
 * Copyright Ambud Sharma
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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.GzipCompressingEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

/**
 * Simple influx client
 *
 */
public class InfluxClientStandalone {

	public static void main(String[] args) throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException,
			ClientProtocolException, IOException, InterruptedException {
		int _THREAD = 4;
		if (args.length >= 1) {
			_THREAD = Integer.parseInt(args[0]);
		}

		String url = "http://" + args[1] + ":8080/influx?db=influx&preSorted=true";

		final String URL = url;

		long ts = System.currentTimeMillis();
		ThreadPoolExecutor es = new ThreadPoolExecutor(_THREAD, _THREAD, Integer.MAX_VALUE, TimeUnit.SECONDS,
				new ArrayBlockingQueue<>(100));
		BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(args[2]))));
		String temp;
		int i = 0;
		StringBuilder builder = new StringBuilder();
		GenericObjectPoolConfig config = new GenericObjectPoolConfig();
		config.setMaxTotal(_THREAD);
		GenericObjectPool<CloseableHttpClient> pool = new GenericObjectPool<>(
				new PooledObjectFactory<CloseableHttpClient>() {

					@Override
					public PooledObject<CloseableHttpClient> makeObject() throws Exception {
						return new DefaultPooledObject<CloseableHttpClient>(buildClient(URL, 30000, 30000));
					}

					@Override
					public void destroyObject(PooledObject<CloseableHttpClient> p) throws Exception {
						p.getObject().close();
					}

					@Override
					public boolean validateObject(PooledObject<CloseableHttpClient> p) {
						return true;
					}

					@Override
					public void activateObject(PooledObject<CloseableHttpClient> p) throws Exception {
					}

					@Override
					public void passivateObject(PooledObject<CloseableHttpClient> p) throws Exception {
					}
				}, config);
		while ((temp = reader.readLine()) != null) {
			builder.append(temp.trim() + "\n");
			if (i % 2000 == 0) {
				final String val = builder.toString();
				builder = new StringBuilder();
				boolean submit = false;
				while (!submit) {
					try {
						es.execute(() -> {
							try {
								CloseableHttpClient client = pool.borrowObject();
								HttpPost post = new HttpPost(url);
								post.setEntity(new GzipCompressingEntity(new StringEntity(val)));
								CloseableHttpResponse execute = client.execute(post);
								if (execute.getStatusLine().getStatusCode() != 204) {
									System.out.println("Bad code:" + execute.getStatusLine().getStatusCode() + "\t"
											+ execute.getStatusLine().getReasonPhrase());
									System.exit(0);
								}
								execute.close();
								pool.returnObject(client);
							} catch (Exception e) {
								e.printStackTrace();
							}
						});
						submit = true;
					} catch (RejectedExecutionException e) {
					}
				}
			}
			if (i % 100000 == 0) {
				System.out.println(i + " (" + new Date()+") elapsed:"+(System.currentTimeMillis() - ts)/1000);
			}
			i++;
		}
		reader.close();
		es.shutdown();
		es.awaitTermination(1000, TimeUnit.SECONDS);
		ts = System.currentTimeMillis() - ts;
		ts = ts / 1000;
		System.out.println("Throughput:" + (i * 10 / ts));
	}

	public static CloseableHttpClient buildClient(String baseURL, int connectTimeout, int requestTimeout)
			throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
		HttpClientBuilder clientBuilder = HttpClients.custom();
		RequestConfig config = RequestConfig.custom().setConnectTimeout(connectTimeout)
				.setConnectionRequestTimeout(requestTimeout).build();

		return clientBuilder.setDefaultRequestConfig(config).build();
	}
}
