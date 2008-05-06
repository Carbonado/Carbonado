/*
 * Copyright 2008 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.gen;

import java.lang.reflect.Method;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.cojen.util.SoftValuedHashMap;

import com.amazon.carbonado.Storable;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableProperty;

/**
 * Basic implementation for {@link Storable#propertyMap} method.
 *
 * @author Brian S O'Neill
 */
public class StorablePropertyMap<S extends Storable> extends AbstractMap<String, Object> {
    private static final Map<Class, Set<String>> cPropertyNamesForType =
        new SoftValuedHashMap();

    public static <S extends Storable> StorablePropertyMap<S> createMap(Class<S> type, S storable)
    {
        Set<String> propertyNames;
        synchronized (cPropertyNamesForType) {
            propertyNames = cPropertyNamesForType.get(type);

            if (propertyNames == null) {
                Map<String, ? extends StorableProperty<S>> properties = 
                    StorableIntrospector.examine(type).getAllProperties();

                for (StorableProperty<S> property : properties.values()) {
                    if (shouldExclude(property)) {
                        if (propertyNames == null) {
                            propertyNames = new LinkedHashSet<String>(properties.keySet());
                        }
                        propertyNames.remove(property.getName());
                        continue;
                    }
                }

                if (propertyNames == null) {
                    propertyNames = properties.keySet();
                } else {
                    propertyNames = Collections.unmodifiableSet(propertyNames);
                }
            }
        }
        return new StorablePropertyMap(propertyNames, storable);
    }

    private static boolean shouldExclude(StorableProperty<?> property) {
        return throwsCheckedException(property.getReadMethod()) ||
            throwsCheckedException(property.getWriteMethod());
    }

    private static boolean throwsCheckedException(Method method) {
        if (method == null) {
            return false;
        }

        Class<?>[] exceptionTypes = method.getExceptionTypes();
        if (exceptionTypes == null) {
            return false;
        }

        for (Class<?> exceptionType : exceptionTypes) {
            if (RuntimeException.class.isAssignableFrom(exceptionType)) {
                continue;
            }
            if (Error.class.isAssignableFrom(exceptionType)) {
                continue;
            }
            return true;
        }

        return false;
    }

    private final Set<String> mPropertyNames;
    private final S mStorable;

    private StorablePropertyMap(Set<String> propertyNames, S storable) {
        mPropertyNames = propertyNames;
        mStorable = storable;
    }

    @Override
    public int size() {
        return mPropertyNames.size();
    }

    @Override
    public boolean isEmpty() {
        // Storables require at least a primary key.
        return false;
    }
    
    @Override
    public boolean containsKey(Object key) {
        return mPropertyNames.contains(key);
    }

    @Override
    public Object get(Object key) {
        try {
            return mStorable.getPropertyValue((String) key);
        } catch (IllegalArgumentException e) {
            // Return null for unknown entries, as per Map specification.
            return null;
        }
    }

    @Override
    public Object put(String key, Object value) {
        Object old = mStorable.getPropertyValue(key);
        mStorable.setPropertyValue(key, value);
        return old;
    }

    @Override
    public Object remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> keySet() {
        return mPropertyNames;
    }

    @Override
    public Collection<Object> values() {
        return new AbstractCollection<Object>() {
            @Override
            public Iterator<Object> iterator() {
                return new Iterator<Object>() {
                    private final Iterator<String> mPropIterator = keySet().iterator();

                    public boolean hasNext() {
                        return mPropIterator.hasNext();
                    }

                    public Object next() {
                        return get(mPropIterator.next());
                    }

                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public int size() {
                return StorablePropertyMap.this.size();
            }

            @Override
            public boolean isEmpty() {
                // Storables require at least a primary key.
                return false;
            }

            @Override
            public boolean contains(Object v) {
                return containsValue(v);
            }

            @Override
            public boolean remove(Object e) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void clear() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public Set<Map.Entry<String, Object>> entrySet() {
        return new AbstractSet<Map.Entry<String, Object>>() {
            @Override
            public Iterator<Map.Entry<String, Object>> iterator() {
                return new Iterator<Map.Entry<String, Object>>() {
                    private final Iterator<String> mPropIterator = keySet().iterator();

                    public boolean hasNext() {
                        return mPropIterator.hasNext();
                    }

                    public Map.Entry<String, Object> next() {
                        final String property = mPropIterator.next();
                        final Object value = get(property);

                        return new Map.Entry<String, Object>() {
                            Object mutableValue = value;

                            public String getKey() {
                                return property;
                            }

                            public Object getValue() {
                                return mutableValue;
                            }

                            public Object setValue(Object value) {
                                Object old = StorablePropertyMap.this.put(property, value);
                                mutableValue = value;
                                return old;
                            }

                            @Override
                            public boolean equals(Object obj) {
                                if (this == obj) {
                                    return true;
                                }

                                if (obj instanceof Map.Entry) {
                                    Map.Entry other = (Map.Entry) obj;

                                    return
                                        (this.getKey() == null ?
                                         other.getKey() == null
                                         : this.getKey().equals(other.getKey()))
                                        &&
                                        (this.getValue() == null ?
                                         other.getValue() == null
                                         : this.getValue().equals(other.getValue()));
                                }

                                return false;
                            }

                            @Override
                            public int hashCode() {
                                return (getKey() == null ? 0 : getKey().hashCode()) ^
                                    (getValue() == null ? 0 : getValue().hashCode());
                            }

                            @Override
                            public String toString() {
                                return property + "=" + mutableValue;
                            }
                        };
                    }

                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public int size() {
                return StorablePropertyMap.this.size();
            }

            @Override
            public boolean isEmpty() {
                // Storables require at least a primary key.
                return false;
            }

            @Override
            public boolean contains(Object e) {
                Map.Entry<String, Object> entry = (Map.Entry<String, Object>) e;
                String key = entry.getKey();
                if (StorablePropertyMap.this.containsKey(key)) {
                    Object value = StorablePropertyMap.this.get(key);
                    return value == null ? entry.getValue() == null
                        : value.equals(entry.getValue());
                }
                return false;
            }

            @Override
            public boolean add(Map.Entry<String, Object> e) {
                StorablePropertyMap.this.put(e.getKey(), e.getValue());
                return true;
            }

            @Override
            public boolean remove(Object e) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void clear() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
