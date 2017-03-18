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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Performs Append operations on a single row.
 * <p>
 * Note that this operation does not appear atomic to readers. Appends are done
 * under a single row lock, so write operations to a row are synchronized, but
 * readers do not take row locks so get and scan operations can see this
 * operation partially completed.
 * <p>
 * To append to a set of columns of a row, instantiate an Append object with the
 * row to append to. At least one column to append must be specified using the
 * {@link #addColumn(byte[], byte[], byte[])} method.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Append extends AddMutation {
  /**
   * @param returnResults
   *          True (default) if the append operation should return the results.
   *          A client that is not interested in the result can save network
   *          bandwidth setting this to false.
   */
  public Append setReturnResults(boolean returnResults) {
    super.setReturnResults(returnResults);
    return this;
  }

  /**
   * @return current setting for returnResults
   */
  // This method makes public the superclasses's protected method.
  public boolean isReturnResults() {
    return super.isReturnResults();
  }

  /**
   * Create a Append operation for the specified row.
   * <p>
   * At least one column must be appended to.
   * @param row row key; makes a local copy of passed in array.
   */
  public Append(byte[] row) {
    this(row, 0, row.length);
  }

  /**
   * Create an Append operation for an immutable row key.
   *
   * @param row the row key
   * @param rowIsImmutable whether the input row is immutable. Set to true if the caller can
   *                       guarantee that the row will not be changed for the Append duration.
   */
  public Append(byte[] row, boolean rowIsImmutable) {
    this(row, HConstants.LATEST_TIMESTAMP, rowIsImmutable);
  }

  /**
   * Create an Append operation for an immutable row key, using a given timestamp.
   *
   * @param row the row key
   * @param timestamp the version timestamp
   * @param rowIsImmutable whether the input row is immutable.
   *                       Set to true if the caller can guarantee that
   *                       the row will not be changed for the Put duration.
   */
  public Append(byte[] row, long timestamp, boolean rowIsImmutable) {
    // Check and set timestamp
    if (ts < 0) {
      throw new IllegalArgumentException("Timestamp cannot be negative. ts=" + ts);
    }
    this.ts = timestamp;

    // Deal with row according to rowIsImmutable
    checkRow(row);
    if (rowIsImmutable) {  // Row is immutable
      this.row = row;  // Do not make a local copy, but point to the provided byte array directly
    } else {  // Row is not immutable
      this.row = Bytes.copy(row, 0, row.length);  // Make a local copy
    }
  }

  /**
   * Copy constructor
   * @param a
   */
  public Append(Append a) {
    this.row = a.getRow();
    this.ts = a.getTimeStamp();
    this.familyMap.putAll(a.getFamilyCellMap());
    for (Map.Entry<String, byte[]> entry : a.getAttributesMap().entrySet()) {
      this.setAttribute(entry.getKey(), entry.getValue());
    }
  }

  /** Create a Append operation for the specified row.
   * <p>
   * At least one column must be appended to.
   * @param rowArray Makes a copy out of this buffer.
   * @param rowOffset
   * @param rowLength
   */
  public Append(final byte [] rowArray, final int rowOffset, final int rowLength) {
    checkRow(rowArray, rowOffset, rowLength);
    this.row = Bytes.copy(rowArray, rowOffset, rowLength);
  }

  /**
   * Add the specified column and value to this Append operation.
   *
   * @param family family name
   * @param qualifier column qualifier
   * @param value value to append to specified column
   * @return this
   * @deprecated since 2.0.0. Use {@link #addColumn(byte[], byte[], byte[])} instead.
   */
  @Deprecated
  public Append add(byte [] family, byte [] qualifier, byte [] value) {
    return addColumn(family, qualifier, value);
  }

  @Override
  public Append add(final Cell cell) {
    // Presume it is KeyValue for now.
    byte [] family = CellUtil.cloneFamily(cell);
    List<Cell> list = this.familyMap.get(family);
    if (list == null) {
      list  = new ArrayList<>(1);
    }
    // find where the new entry should be placed in the List
    list.add(cell);
    this.familyMap.put(family, list);
    return this;
  }

  @Override
  public Append addColumn(byte[] family, byte[] qualifier, byte[] value) {
    return (Append) super.addColumn(family, qualifier, value);
  }

  @Override
  public Append addColumn(byte[] family, byte[] qualifier, long timestamp, byte[] value) {
    return (Append) super.addColumn(family, qualifier, timestamp, value);
  }

  @Override
  public Append addColumn(byte[] family, ByteBuffer qualifier, long timestamp, ByteBuffer value) {
    return (Append) super.addColumn(family, qualifier, timestamp, value);
  }

  @Override
  public Append addImmutable(byte[] family, byte[] qualifier, byte[] value) {
    return (Append) super.addImmutable(family, qualifier, value);
  }

  @Override
  public Append addImmutable(byte[] family, byte[] qualifier, long timestamp, byte[] value) {
    return (Append) super.addImmutable(family, qualifier, timestamp, value);
  }

  @Override
  public Append addImmutable(byte[] family, byte[] qualifier, byte[] value, Tag[] tag) {
    return (Append) super.addImmutable(family, qualifier, value, tag);
  }

  @Override
  public Append addImmutable(byte[] family, byte[] qualifier, long timestamp, byte[] value, Tag[] tag) {
    return (Append) super.addImmutable(family, qualifier, timestamp, value, tag);
  }

  @Override
  public Append addImmutable(byte[] family, ByteBuffer qualifier, long timestamp, ByteBuffer value) {
    return (Append) super.addImmutable(family, qualifier, timestamp, value);
  }

  @Override
  public Append addImmutable(byte[] family, ByteBuffer qualifier, long timestamp, ByteBuffer value, Tag[] tag) {
    return (Append) super.addImmutable(family, qualifier, timestamp, value, tag);
  }

  @Override
  public Append setAttribute(String name, byte[] value) {
    return (Append) super.setAttribute(name, value);
  }

  @Override
  public Append setId(String id) {
    return (Append) super.setId(id);
  }

  @Override
  public Append setDurability(Durability d) {
    return (Append) super.setDurability(d);
  }

  @Override
  public Append setFamilyCellMap(NavigableMap<byte[], List<Cell>> map) {
    return (Append) super.setFamilyCellMap(map);
  }

  @Override
  public Append setClusterIds(List<UUID> clusterIds) {
    return (Append) super.setClusterIds(clusterIds);
  }

  @Override
  public Append setCellVisibility(CellVisibility expression) {
    return (Append) super.setCellVisibility(expression);
  }

  @Override
  public Append setACL(String user, Permission perms) {
    return (Append) super.setACL(user, perms);
  }

  @Override
  public Append setACL(Map<String, Permission> perms) {
    return (Append) super.setACL(perms);
  }

  @Override
  public Append setTTL(long ttl) {
    return (Append) super.setTTL(ttl);
  }
}
