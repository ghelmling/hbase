/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import java.io.IOException;

/**
 * Coprocessors implement this interface to observe and mediate client actions
 * on the region.
 */
public interface RegionObserver {

  /**
   * Called before a client makes a GetClosestRowBefore request.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param e the environment provided by the region server
   * @param row the row
   * @param family the family
   * @param result The result to return to the client if default processing
   * is bypassed. Can be modified. Will not be used if default processing
   * is not bypassed.
   * @throws IOException if an error occurred on the coprocessor
   */
  public void preGetClosestRowBefore(final CoprocessorEnvironment e,
      final byte [] row, final byte [] family, final Result result)
    throws IOException;

  /**
   * Called after a client makes a GetClosestRowBefore request.
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param e the environment provided by the region server
   * @param row the row
   * @param family the desired family
   * @param result the result to return to the client, modify as necessary
   * @throws IOException if an error occurred on the coprocessor
   */
  public void postGetClosestRowBefore(final CoprocessorEnvironment e,
      final byte [] row, final byte [] family, final Result result)
    throws IOException;

  /**
   * Called before the client performs a Get
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param e the environment provided by the region server
   * @param get the Get request
   * @param result The result to return to the client if default processing
   * is bypassed. Can be modified. Will not be used if default processing
   * is not bypassed.
   * @throws IOException if an error occurred on the coprocessor
   */
  public void preGet(final CoprocessorEnvironment e, final Get get,
      final List<KeyValue> result)
    throws IOException;

  /**
   * Called after the client performs a Get
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param e the environment provided by the region server
   * @param get the Get request
   * @param result the result to return to the client, modify as necessary
   * @throws IOException if an error occurred on the coprocessor
   */
  public void postGet(final CoprocessorEnvironment e, final Get get,
      final List<KeyValue> result)
    throws IOException;

  /**
   * Called before the client tests for existence using a Get.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param e the environment provided by the region server
   * @param get the Get request
   * @param exists
   * @return the value to return to the client if bypassing default processing
   * @throws IOException if an error occurred on the coprocessor
   */
  public boolean preExists(final CoprocessorEnvironment e, final Get get,
      final boolean exists)
    throws IOException;

  /**
   * Called after the client tests for existence using a Get.
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param e the environment provided by the region server
   * @param get the Get request
   * @param exists the result returned by the region server
   * @return the result to return to the client
   * @throws IOException if an error occurred on the coprocessor
   */
  public boolean postExists(final CoprocessorEnvironment e, final Get get,
      final boolean exists)
    throws IOException;

  /**
   * Called before the client stores a value.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param e the environment provided by the region server
   * @param familyMap map of family to edits for the given family
   * @param writeToWAL true if the change should be written to the WAL
   * @throws IOException if an error occurred on the coprocessor
   */
  public void prePut(final CoprocessorEnvironment e, final Map<byte[],
      List<KeyValue>> familyMap, final boolean writeToWAL)
    throws IOException;

  /**
   * Called after the client stores a value.
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param e the environment provided by the region server
   * @param familyMap map of family to edits for the given family
   * @param writeToWAL true if the change should be written to the WAL
   * @throws IOException if an error occurred on the coprocessor
   */
  public void postPut(final CoprocessorEnvironment e, final Map<byte[],
      List<KeyValue>> familyMap, final boolean writeToWAL)
    throws IOException;

  /**
   * Called before the client deletes a value.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param e the environment provided by the region server
   * @param familyMap map of family to edits for the given family
   * @param writeToWAL true if the change should be written to the WAL
   * @throws IOException if an error occurred on the coprocessor
   */
  public void preDelete(final CoprocessorEnvironment e, final Map<byte[],
      List<KeyValue>> familyMap, final boolean writeToWAL)
    throws IOException;

  /**
   * Called after the client deletes a value.
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param e the environment provided by the region server
   * @param familyMap map of family to edits for the given family
   * @param writeToWAL true if the change should be written to the WAL
   * @throws IOException if an error occurred on the coprocessor
   */
  public void postDelete(final CoprocessorEnvironment e,
      final Map<byte[], List<KeyValue>> familyMap, final boolean writeToWAL)
    throws IOException;

  /**
   * Called before checkAndPut
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param e the environment provided by the region server
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param value the expected value
   * @param put data to put if check succeeds
   * @param result 
   * @return the return value to return to client if bypassing default
   * processing
   * @throws IOException if an error occurred on the coprocessor
   */
  public boolean preCheckAndPut(final CoprocessorEnvironment e,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final byte [] value, final Put put, final boolean result)
    throws IOException;

  /**
   * Called after checkAndPut
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param e the environment provided by the region server
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param value the expected value
   * @param put data to put if check succeeds
   * @param result from the checkAndPut
   * @return the possibly transformed return value to return to client
   * @throws IOException if an error occurred on the coprocessor
   */
  public boolean postCheckAndPut(final CoprocessorEnvironment e,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final byte [] value, final Put put, final boolean result)
    throws IOException;

  /**
   * Called before checkAndDelete
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param e the environment provided by the region server
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param value the expected value
   * @param delete delete to commit if check succeeds
   * @param result 
   * @return the value to return to client if bypassing default processing
   * @throws IOException if an error occurred on the coprocessor
   */
  public boolean preCheckAndDelete(final CoprocessorEnvironment e,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final byte [] value, final Delete delete, final boolean result)
    throws IOException;

  /**
   * Called after checkAndDelete
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param e the environment provided by the region server
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param value the expected value
   * @param delete delete to commit if check succeeds
   * @param result from the CheckAndDelete
   * @return the possibly transformed returned value to return to client
   * @throws IOException if an error occurred on the coprocessor
   */
  public boolean postCheckAndDelete(final CoprocessorEnvironment e,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final byte [] value, final Delete delete, final boolean result)
    throws IOException;

  /**
   * Called before incrementColumnValue
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param e the environment provided by the region server
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param amount long amount to increment
   * @param writeToWAL true if the change should be written to the WAL
   * @return value to return to the client if bypassing default processing
   * @throws IOException if an error occurred on the coprocessor
   */
  public long preIncrementColumnValue(final CoprocessorEnvironment e,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final long amount, final boolean writeToWAL)
    throws IOException;

  /**
   * Called after incrementColumnValue
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param e the environment provided by the region server
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param amount long amount to increment
   * @param writeToWAL true if the change should be written to the WAL
   * @param result the result returned by incrementColumnValue
   * @return the result to return to the client
   * @throws IOException if an error occurred on the coprocessor
   */
  public long postIncrementColumnValue(final CoprocessorEnvironment e,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final long amount, final boolean writeToWAL, final long result)
    throws IOException;

  /**
   * Called before Increment
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param e the environment provided by the region server
   * @param increment increment object
   * @param result The result to return to the client if default processing
   * is bypassed. Can be modified. Will not be used if default processing
   * is not bypassed.
   * @param writeToWAL true if the change should be written to the WAL
   * @throws IOException if an error occurred on the coprocessor
   */
  public void preIncrement(final CoprocessorEnvironment e,
      final Increment increment, final Result result)
    throws IOException;

  /**
   * Called after increment
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param e the environment provided by the region server
   * @param increment increment object
   * @param writeToWAL true if the change should be written to the WAL
   * @param result the result returned by increment, can be modified
   * @throws IOException if an error occurred on the coprocessor
   */
  public void postIncrement(final CoprocessorEnvironment e,
      final Increment increment, final Result result)
    throws IOException;

  /**
   * Called before the client opens a new scanner.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param e the environment provided by the region server
   * @param scan the Scan specification
   * @param s if not null, the base scanner
   * @return an InternalScanner instance to use instead of the base scanner if
   * overriding default behavior, null otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public InternalScanner preScannerOpen(final CoprocessorEnvironment e,
      final Scan scan, final InternalScanner s)
    throws IOException;

  /**
   * Called after the client opens a new scanner.
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param e the environment provided by the region server
   * @param scan the Scan specification
   * @param s if not null, the base scanner
   * @return the scanner instance to use
   * @throws IOException if an error occurred on the coprocessor
   */
  public InternalScanner postScannerOpen(final CoprocessorEnvironment e,
      final Scan scan, final InternalScanner s)
    throws IOException;

  /**
   * Called before the client asks for the next row on a scanner.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param e the environment provided by the region server
   * @param s the scanner
   * @param result The result to return to the client if default processing
   * is bypassed. Can be modified. Will not be returned if default processing
   * is not bypassed.
   * @param limit the maximum number of results to return
   * @param hasNext the 'has more' indication
   * @return 'has more' indication that should be sent to client
   * @throws IOException if an error occurred on the coprocessor
   */
  public boolean preScannerNext(final CoprocessorEnvironment e,
      final InternalScanner s, final List<KeyValue> result,
      final int limit, final boolean hasNext)
    throws IOException;

  /**
   * Called after the client asks for the next row on a scanner.
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param e the environment provided by the region server
   * @param s the scanner
   * @param result the result to return to the client, can be modified
   * @param limit the maximum number of results to return
   * @param hasNext the 'has more' indication
   * @return 'has more' indication that should be sent to client
   * @throws IOException if an error occurred on the coprocessor
   */
  public boolean postScannerNext(final CoprocessorEnvironment e,
      final InternalScanner s, final List<KeyValue> result, final int limit,
      final boolean hasNext)
    throws IOException;

  /**
   * Called before the client closes a scanner.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param e the environment provided by the region server
   * @param s the scanner
   * @throws IOException if an error occurred on the coprocessor
   */
  public void preScannerClose(final CoprocessorEnvironment e,
      final InternalScanner s)
    throws IOException;

  /**
   * Called after the client closes a scanner.
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained
   * coprocessors
   * @param e the environment provided by the region server
   * @param s the scanner
   * @throws IOException if an error occurred on the coprocessor
   */
  public void postScannerClose(final CoprocessorEnvironment e,
      final InternalScanner s)
    throws IOException;
}
