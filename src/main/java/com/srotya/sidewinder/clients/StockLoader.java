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
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
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

public class StockLoader {

	private static final String URL = "http://localhost:8080/database/stocks/measurement/ticker/series/ticker";

	public static void main(String[] args) throws IOException, KeyManagementException, NoSuchAlgorithmException,
			KeyStoreException, ParseException, InterruptedException {
		long ts1 = System.currentTimeMillis();
		ExecutorService es = Executors.newFixedThreadPool(2);

		int counter = 0;
		File[] listFiles = new File(args[0]).listFiles();
		for (int i = 0; i < listFiles.length; i++) {
			File file = listFiles[i];
			CloseableHttpClient client = buildClient(URL, 30000, 30000);
			String ticker = file.getName().replace(".csv", "").split("_")[1];
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String temp = null;
			SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
			StringBuilder req = new StringBuilder();
			long ts = System.currentTimeMillis() - 100 * 3600_000;

			/*
			 * while ((temp = reader.readLine()) != null) { String[] splits =
			 * temp.split(","); // Date timestamp = format.parse(splits[0]); // ts =
			 * timestamp.getTime();
			 * 
			 * String metric = "ticker," + ticker + " open=" + splits[2] + ",high=" +
			 * splits[3] + ",low=" + splits[4] + ",close=" + splits[5] + ",volume=" +
			 * splits[6] + "i " + ts * 1000 * 1000; req.append(metric + "\n"); ts += 1000;
			 * counter++; System.out.println(counter); // if (counter >= 100) { // break; //
			 * } }
			 */

			while ((temp = reader.readLine()) != null) {
				String[] splits = temp.split(",");
				Date timestamp = format.parse(splits[0]);
				ts = timestamp.getTime();
				// ts = ts + 1000 * 5;

				String metric = "ticker,symbol=" + ticker + " open=" + splits[2] + ",high=" + splits[3] + ",low=" + splits[4]
						+ ",close=" + splits[5] + ",volume=" + splits[6] + " " + ts * 1000 * 1000;
				req.append(metric + "\n");
				counter++;
			}
			es.submit(() -> {
				HttpPost post = new HttpPost("http://localhost:8080/influx?db=stocks&replica=1");
				try {
					post.setEntity(new StringEntity(req.toString()));
					CloseableHttpResponse execute = client.execute(post);
					if (execute.getStatusLine().getStatusCode() != 204) {
						System.out.println("Bad code:" + execute.getStatusLine().getStatusCode() + "\t"
								+ execute.getStatusLine().getReasonPhrase());
						System.out.println(req.toString());
						System.exit(0);
					}
					execute.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
			reader.close();
			// Thread.sleep(10);
		}
		es.shutdown();
		es.awaitTermination(1000, TimeUnit.SECONDS);
		ts1 = System.currentTimeMillis() - ts1;
		System.out.println(counter + "  " + ts1);
	}

	public static CloseableHttpClient buildClient(String baseURL, int connectTimeout, int requestTimeout)
			throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
		HttpClientBuilder clientBuilder = HttpClients.custom();
		RequestConfig config = RequestConfig.custom().setConnectTimeout(connectTimeout)
				.setConnectionRequestTimeout(requestTimeout).build();

		return clientBuilder.setDefaultRequestConfig(config).build();
	}
}
