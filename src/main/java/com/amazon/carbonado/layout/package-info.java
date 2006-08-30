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
 * Support for recording the evolution of a storable's layout, used internally
 * by some repositories. This allows storable's to evolve. Enough information
 * is recorded in the {@link com.amazon.carbonado.layout.Layout layout} such
 * that an older generation can be reconstructed, allowing it to be decoded
 * from persistent storage.
 *
 * <p>A storable generation is different than a storable {@link
 * com.amazon.carbonado.Version version}. The version increases with each
 * update of an <i>instance</i>, whereas the generation increases when the
 * storable type definition changes. The version number is stored with each
 * instance, and the generation is stored via the classes in this package.
 *
 * <p>Whenever a property is added or removed from a storable, the storable
 * layout is assigned a new generation value. If the storable layout reverts to
 * a previous generation's layout, no new generation value is created. Instead,
 * the generation value of the current storable will match the previous
 * generation.
 *
 * @see com.amazon.carbonado.layout.LayoutFactory
 */
package com.amazon.carbonado.layout;
