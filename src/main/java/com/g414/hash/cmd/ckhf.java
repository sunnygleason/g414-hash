/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.g414.hash.cmd;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.logging.Logger;

import com.g414.hash.file2.HashFile2;

public class ckhf {
    private static final Logger log = Logger.getLogger(ckhf.class.getName());

    public static void main(String[] args) throws Exception {
        LinkedList<String> theArgs = new LinkedList<String>();
        theArgs.addAll(Arrays.asList(args));

        log.info("loading...");
        HashFile2 hf = new HashFile2(theArgs.removeFirst(), true);

        boolean lower = Boolean.valueOf(System.getProperty("lower", "false"));
        String delim = System.getProperty("delim", "\t");

        long j = 0;
        for (String file : theArgs) {
            log.info("processing: " + file);

            Scanner x = new Scanner(new File(file));
            long i = 0;
            while (x.hasNextLine()) {
                String m = x.nextLine();
                if (lower) {
                    m = m.toLowerCase();
                }

                Iterable<byte[]> values = hf.getMulti(m.getBytes("UTF-8"));
                for (byte[] value : values) {
                    System.out.println(m + delim + new String(value, "UTF-8"));
                }

                if (i % 100000 == 0) {
                    log.info(file + " : " + i + " " + j + " " + m);
                }

                i += 1;
            }
        }

        log.info("done.");
    }
}
