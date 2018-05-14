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

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.concurrent.ThreadLocalRandom;

public class DataGen {

	public static void main(String[] args) throws FileNotFoundException {
		PrintWriter pr = new PrintWriter("./target/data.txt");
		long baseTs = System.currentTimeMillis();

		ThreadLocalRandom rand = ThreadLocalRandom.current();
		for (int i = 0; i < 400000; i++) {
			pr.println("cpu,hostname=host_23" + (i % 1000) + ",region=us-west-1,os=ubunut10.1 usage_user="
					+ 58 * rand.nextInt(100) + " " + (baseTs + i * 10) * 1000 * 1000);
		}
		pr.close();
	}

}
