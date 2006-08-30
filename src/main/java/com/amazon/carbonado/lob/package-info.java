/*
 * Copyright 2006 Amazon Technologies, Inc. or its affiliates.
 * Amazon, Amazon.com and Carbonado are trademarks or registered trademarks
 * of Amazon Technologies, Inc. or its affiliates.  All rights reserved.
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

/**
 * Support for LOB property types, which are Large OBjects. Properties declared
 * as {@link com.amazon.carbonado.lob.Blob Blob} or {@link
 * com.amazon.carbonado.lob.Clob Clob} are treated differently than regular
 * properties. In particular:
 *
 * <ul>
 * <li>Repository typically stores LOB data external from enclosing storable
 * <li>LOBs are accessed in a manner similar to how files are accessed
 * <li>LOB data is often read/written in chunks, so consider accessing in a transaction scope
 * <li>LOBs cannot be annotated with {@link com.amazon.carbonado.PrimaryKey
 * PrimaryKey}, {@link com.amazon.carbonado.Key Key}, {@link
 * com.amazon.carbonado.Index Index}, {@link com.amazon.carbonado.Join Join},
 * {@link com.amazon.carbonado.Version Version}, or {@link
 * com.amazon.carbonado.Sequence Sequence}
 * <li>LOBs cannot be used in a {@link com.amazon.carbonado.Storage#query(String) query filter}
 * </ul>
 *
 * <p>Also, setting a LOB property does not dirty that property unless the new
 * LOB is unequal. Updating a LOB property typically involves operating on the
 * LOB itself. Setting the LOB property again is useful only when completely
 * replacing the data, which can be a relatively expensive operation.
 *
 * <p>Some repositories require that large text data be stored as a LOB. If the
 * text property is intended to fit entirely in memory, consider defining the
 * property as a String instead of a LOB. This allows the repository to decide
 * if it is appropriate to store it as a LOB. If explicit control over charset
 * encoding is required, add a {@link com.amazon.carbonado.adapter.TextAdapter
 * TextAdapter} annotation.
 */
package com.amazon.carbonado.lob;
