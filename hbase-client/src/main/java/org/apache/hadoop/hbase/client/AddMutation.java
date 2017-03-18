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

package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

import java.nio.ByteBuffer;
import java.util.List;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class AddMutation extends Mutation {
    /**
     * Adds the specified column and value to this operation.
     *
     * @param family the name of the column family
     * @param qualifier the qualifier of the column
     * @param value the value of the column
     * @return this
     */
    public AddMutation addColumn(byte[] family, byte[] qualifier, byte[] value) {
        return addColumn(family, qualifier, this.ts, value);
    }

    /**
     * Add the specified column and value, with the specified timestamp as its version to this
     * operation.
     *
     * @param family the name of the column family
     * @param qualifier the qualifier of the column
     * @param timestamp the version timestamp
     * @param value the value of the column
     * @return this
     */
    public AddMutation addColumn(byte[] family, byte[] qualifier, long timestamp, byte[] value) {
        if (family == null) {
            throw new IllegalArgumentException("Family cannot be null");
        }

        if (timestamp < 0) {
            throw new IllegalArgumentException("Timestamp cannot be negative. ts=" + timestamp);
        }

        List<Cell> list = getCellList(family);
        KeyValue kv = createPutKeyValue(family, qualifier, timestamp, value);
        list.add(kv);
        familyMap.put(CellUtil.cloneFamily(kv), list);

        return this;
    }

    /**
     * Add the specified column and value, with the specified timestamp as its version to this
     * operation.
     *
     * @param family the name of the column family
     * @param qualifier the qualifier of the column
     * @param timestamp the version timestamp
     * @param value the value of the column
     * @return this
     */
    public AddMutation addColumn(byte[] family, ByteBuffer qualifier, long timestamp, ByteBuffer value) {
        if (family == null) {
            throw new IllegalArgumentException("Family cannot be null");
        }

        if (timestamp < 0) {
            throw new IllegalArgumentException("Timestamp cannot be negative. ts=" + timestamp);
        }

        List<Cell> list = getCellList(family);
        KeyValue kv = createPutKeyValue(family, qualifier, timestamp, value, null);
        list.add(kv);
        familyMap.put(CellUtil.cloneFamily(kv), list);

        return this;
    }

    /**
     * This version expects that the underlying arrays won't change. It's intended for usage
     * internal HBase to and for advanced client applications.
     *
     * @param family the name of the column family
     * @param qualifier the qualifier of the column
     * @param value the value of the column
     * @return this
     * @see #addColumn(byte[], byte[], byte[])
     */
    public AddMutation addImmutable(byte[] family, byte[] qualifier, byte[] value) {
        return addImmutable(family, qualifier, this.ts, value);
    }

    /**
     * This version expects that the underlying arrays won't change. It's intended for usage
     * internal HBase to and for advanced client applications.
     *
     * @param family the name of the column family
     * @param qualifier the qualifier of the column
     * @param timestamp the version timestamp
     * @param value the value of the column
     * @return this
     * @see #addColumn(byte[], byte[], long, byte[])
     */
    public AddMutation addImmutable(byte[] family, byte[] qualifier, long timestamp, byte[] value) {
        return addImmutable(family, qualifier, timestamp, value, null);
    }

    /**
     * This expects that the underlying arrays won't change. It's intended for usage internal HBase
     * to and for advanced client applications.
     * <p>Marked as audience Private as of 1.2.0. {@link Tag} is an internal implementation detail
     * that should not be exposed publicly.
     *
     * @param family the name of the column family
     * @param qualifier the qualifier for the column
     * @param value the value of the column
     * @param tag {@link Tag}s associated with the column
     * @return this
     */
    @InterfaceAudience.Private
    public AddMutation addImmutable(byte[] family, byte[] qualifier, byte[] value, Tag[] tag) {
        return addImmutable(family, qualifier, this.ts, value, tag);
    }

    /**
     * This version expects that the underlying arrays won't change. It's intended for usage
     * internal HBase to and for advanced client applications.
     *
     * @param family the name of the column family
     * @param qualifier the qualifier for the column
     * @param timestamp the version timestamp
     * @param value the value of the column
     * @return this
     * @see #addColumn(byte[], ByteBuffer, long, ByteBuffer)
     */
    @InterfaceAudience.Private
    public AddMutation addImmutable(byte[] family, ByteBuffer qualifier, long timestamp, ByteBuffer value) {
        return addImmutable(family, qualifier, timestamp, value, null);
    }

    /**
     * This expects that the underlying arrays won't change. It's intended for usage internal HBase
     * to and for advanced client applications.
     * <p>Marked as audience Private as of 1.2.0. {@link Tag} is an internal implementation detail
     * that should not be exposed publicly.
     *
     * @param family the name of the column family
     * @param qualifier the qualifier for the column
     * @param timestamp the version timestamp
     * @param value the value of the column
     * @param tag {@link Tag}s associated with the column
     * @return this
     */
    @InterfaceAudience.Private
    public AddMutation addImmutable(byte[] family, byte[] qualifier, long timestamp, byte[] value, Tag[] tag) {
        if (family == null) {
            throw new IllegalArgumentException("Family cannot be null");
        }

        if (timestamp < 0) {
            throw new IllegalArgumentException("Timestamp cannot be negative. ts=" + timestamp);
        }

        List<Cell> list = getCellList(family);
        KeyValue kv = createPutKeyValue(family, qualifier, timestamp, value, tag);
        list.add(kv);
        familyMap.put(family, list);

        return this;
    }

    /**
     * This expects that the underlying arrays won't change. It's intended for usage internal HBase
     * to and for advanced client applications.
     * <p>Marked as audience Private as of 1.2.0. {@link Tag} is an internal implementation detail
     * that should not be exposed publicly.
     *
     * @param family the name of the column family
     * @param qualifier the qualifier for the column
     * @param timestamp the version timestamp
     * @param value the value of the column
     * @param tag {@link Tag}s associated with the column
     * @return this
     */
    @InterfaceAudience.Private
    public AddMutation addImmutable(byte[] family, ByteBuffer qualifier, long timestamp, ByteBuffer value, Tag[] tag) {
        if (family == null) {
            throw new IllegalArgumentException("Family cannot be null");
        }

        if (timestamp < 0) {
            throw new IllegalArgumentException("Timestamp cannot be negative. ts=" + timestamp);
        }

        List<Cell> list = getCellList(family);
        KeyValue kv = createPutKeyValue(family, qualifier, timestamp, value, tag);
        list.add(kv);
        familyMap.put(family, list);

        return this;
    }
}
