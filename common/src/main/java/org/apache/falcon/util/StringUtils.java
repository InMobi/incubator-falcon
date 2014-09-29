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

package org.apache.falcon.util;

/**
 * Custom String Utils.
 */
public final class StringUtils {

    private StringUtils() { }

    //Hadoop2.4.0 has commons-lang2.5 which doesn't contain repeat(). Hence using custom repeat()
    public static String repeat(String repeatStr, String delim, int cnt) {
        String[] array = new String[cnt];
        for (int index = 0; index < cnt; index++) {
            array[index] = repeatStr;
        }
        return org.apache.commons.lang.StringUtils.join(array, delim);
    }
}
