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
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.logging.Logger;

import com.g414.hash.bloom.BloomFilter;
import com.g414.hash.bloom.FilterState;

public class ckblm {
    private static final Logger log = Logger.getLogger(ckblm.class.getName());

    public static void main(String[] args) throws Exception {
        LinkedList<String> theArgs = new LinkedList<String>();
        theArgs.addAll(Arrays.asList(args));

        log.info("loading...");
        ObjectInputStream in = new ObjectInputStream(new FileInputStream(
                theArgs.removeFirst()));

        FilterState state = (FilterState) in.readObject();
        in.close();

        BloomFilter bloom = new BloomFilter(state);

        boolean reverse = Boolean.valueOf(System
                .getProperty("reverse", "false"));
        boolean lower = Boolean.valueOf(System.getProperty("lower", "false"));

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

                boolean hasEntry = bloom.contains(m);

                if (hasEntry == reverse) {
                    System.out.println(m);
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
