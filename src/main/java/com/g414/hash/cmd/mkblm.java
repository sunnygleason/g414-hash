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
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.logging.Logger;

import com.g414.hash.bloom.BloomFilter;
import com.g414.hash.impl.MurmurHash;

public class mkblm {
    private static final Logger log = Logger.getLogger(mkblm.class.getName());

    public static void main(String[] args) throws Exception {
        LinkedList<String> theArgs = new LinkedList<String>();
        theArgs.addAll(Arrays.asList(args));

        String outFile = theArgs.removeFirst();
        long expectedElements = Long.parseLong(theArgs.removeFirst());
        int bitsPerElement = Integer.parseInt(theArgs.removeFirst());

        BloomFilter bloom = new BloomFilter(new MurmurHash(), expectedElements,
                bitsPerElement, true);

        log.info("adding...");

        boolean lower = Boolean.valueOf(System.getProperty("lower", "false"));

        long j = 0;
        for (String file : theArgs) {
            long i = 0;
            Scanner x = new Scanner(new File(file));
            while (x.hasNextLine()) {
                String n = x.nextLine();
                if (lower) {
                    n = n.toLowerCase();
                }

                i += 1;

                bloom.put(n);

                if (i % 100000 == 0) {
                    log.info(file + " : " + i + " " + j + " " + n);
                }

                j += 1;
            }
        }

        log.info(j + " saving...");
        ObjectOutputStream o = new ObjectOutputStream(new FileOutputStream(
                outFile));
        o.writeObject(bloom.getState());
        o.close();
        log.info(j + " done.");
    }
}
