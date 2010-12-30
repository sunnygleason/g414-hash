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

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.logging.Logger;

import com.g414.hash.file2.HashEntry;
import com.g414.hash.file2.HashFile2;

public class dumphf {
    private static final Logger log = Logger.getLogger(dumphf.class.getName());

    public static void main(String[] args) throws Exception {
        LinkedList<String> theArgs = new LinkedList<String>();
        theArgs.addAll(Arrays.asList(args));

        String inFile = theArgs.removeFirst();
        String outFile = theArgs.removeFirst();

        String delim = System.getProperty("delim", "\t");

        PrintWriter out = new PrintWriter(outFile);

        long i = 0;
        for (HashEntry entry : HashFile2.elements(inFile)) {
            String n = new String(entry.getKey(), "UTF-8") + delim
                    + new String(entry.getValue(), "UTF-8");

            out.println(n);

            if (i % 100000 == 0) {
                log.info(inFile + " : " + i + " " + n);
            }
        }

        out.close();
        log.info("done.");
    }
}
