/*
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
package org.apache.hadoop.hbase.classification.tools;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestStabilityOptions {
    @Test
    public void testFilterOptionsReturnsOption() {
        String[][] options = new String[4][];
        options[0] = new String[] { "-unstable" };
        options[1] = new String[] { "-stable" };
        options[2] = new String[] { "-evolving" };
        options[3] = new String[] { "-test" };

        String[][] filteredOptions = StabilityOptions.filterOptions(options);

        String[] expected = new String[] { "-test" };
        Assert.assertArrayEquals(expected, filteredOptions[0]);
    }

    @Test
    public void testFilterOptionsReturnsNoOptions() {
        String[][] options = new String[3][];
        options[0] = new String[] { "-unstable" };
        options[1] = new String[] { "-stable" };
        options[2] = new String[] { "-evolving" };

        String[][] filteredOptions = StabilityOptions.filterOptions(options);

        Assert.assertTrue(filteredOptions.length == 0);
    }
}
