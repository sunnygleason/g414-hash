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

import com.g414.hash.file2.ByteSize;
import com.g414.hash.file2.HashFile2Builder;

public class mkhf {
    private static final Logger log = Logger.getLogger(mkhf.class.getName());

    public static void main(String[] args) throws Exception {
        LinkedList<String> theArgs = new LinkedList<String>();
        theArgs.addAll(Arrays.asList(args));

        String outFile = theArgs.removeFirst();
        long expectedElements = Long.parseLong(theArgs.removeFirst());

        boolean lower = Boolean.valueOf(System.getProperty("lower", "false"));
        String delim = System.getProperty("delim", "\t");

        ByteSize keySize = ByteSize.valueOf(System.getProperty("keySize",
                "FOUR"));
        ByteSize valueSize = ByteSize.valueOf(System.getProperty("valueSize",
                "FOUR"));

        boolean isAssociative = Boolean.valueOf(System.getProperty(
                "isAssociative", "false"));
        boolean isLongHash = Boolean.valueOf(System.getProperty("isLongHash",
                "true"));
        boolean isLargeCapacity = Boolean.valueOf(System.getProperty(
                "isLargeCapacity", "true"));
        boolean isLargeFile = Boolean.valueOf(System.getProperty("isLargeFile",
                "true"));

        HashFile2Builder hf = new HashFile2Builder(isAssociative, outFile,
                expectedElements, keySize, valueSize, isLongHash,
                isLargeCapacity, isLargeFile);

        log.info("adding...");

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

                String[] v = n.split(delim);
                if (v.length == 2) {
                    hf.add(v[0].getBytes("UTF-8"), v[1].getBytes("UTF-8"));
                } else {
                    log.fine("BAD line : " + n);
                }

                if (i % 100000 == 0) {
                    log.info(file + " : " + i + " " + j + " " + n);
                }

                j += 1;
            }
        }

        log.info(j + " building...");
        hf.finish();
        log.info(j + " done.");
    }
}
