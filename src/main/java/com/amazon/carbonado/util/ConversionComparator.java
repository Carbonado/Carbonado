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

package com.amazon.carbonado.util;

import java.util.Comparator;

import org.cojen.classfile.TypeDesc;

/**
 * Compares type conversions, finding the one that is nearest.
 *
 * @author Brian S O'Neill
 * @since 1.2
 */
public class ConversionComparator implements Comparator<Class> {
    private final TypeDesc mFrom;

    public ConversionComparator(Class fromType) {
        mFrom = TypeDesc.forClass(fromType);
    }

    /**
     * Returns true if a coversion is possible to the given type.
     */
    public boolean isConversionPossible(Class toType) {
        return isConversionPossible(mFrom, TypeDesc.forClass(toType));
    }

    @SuppressWarnings("unchecked")
    private static boolean isConversionPossible(TypeDesc from, TypeDesc to) {
        if (from == to) {
            return true;
        }

        if (from.toPrimitiveType() != null && to.toPrimitiveType() != null) {
            from = from.toPrimitiveType();
            to = to.toPrimitiveType();
        } else {
            from = from.toObjectType();
            to = to.toObjectType();
        }

        switch (from.getTypeCode()) {
        case TypeDesc.OBJECT_CODE: default:
            return to.toClass().isAssignableFrom(from.toClass());
        case TypeDesc.BOOLEAN_CODE:
            return to == TypeDesc.BOOLEAN;
        case TypeDesc.BYTE_CODE:
            return to == TypeDesc.BYTE || to == TypeDesc.SHORT
                || to == TypeDesc.INT || to == TypeDesc.LONG
                || to == TypeDesc.FLOAT || to == TypeDesc.DOUBLE;
        case TypeDesc.SHORT_CODE:
            return to == TypeDesc.SHORT
                || to == TypeDesc.INT || to == TypeDesc.LONG
                || to == TypeDesc.FLOAT || to == TypeDesc.DOUBLE;
        case TypeDesc.CHAR_CODE:
            return to == TypeDesc.CHAR;
        case TypeDesc.INT_CODE:
            return to == TypeDesc.INT || to == TypeDesc.LONG || to == TypeDesc.DOUBLE;
        case TypeDesc.FLOAT_CODE:
            return to == TypeDesc.FLOAT || to == TypeDesc.DOUBLE;
        case TypeDesc.LONG_CODE:
            return to == TypeDesc.LONG;
        case TypeDesc.DOUBLE_CODE:
            return to == TypeDesc.DOUBLE;
        }
    }

    /**
     * Evaluates two types, to see which one is nearest to the from type.
     * Return {@literal <0} if "a" is nearest, 0 if both are equally good,
     * {@literal >0} if "b" is nearest.
     */
    public int compare(Class toType_a, Class toType_b) {
        TypeDesc from = mFrom;
        TypeDesc a = TypeDesc.forClass(toType_a);
        TypeDesc b = TypeDesc.forClass(toType_b);

        if (from == a) {
            if (from == b) {
                return 0;
            }
            return -1;
        } else if (from == b) {
            return 1;
        }

        int result = compare(from, a, b);
        if (result != 0) {
            return result;
        }

        if (from.isPrimitive()) {
            // Try boxing.
            if (from.toObjectType() != null) {
                from = from.toObjectType();
                return compare(from, a, b);
            }
        } else {
            // Try unboxing.
            if (from.toPrimitiveType() != null) {
                from = from.toPrimitiveType();
                result = compare(from, a, b);
                if (result != 0) {
                    return result;
                }
                // Try boxing back up. Test by unboxing 'to' types.
                if (!toType_a.isPrimitive() && a.toPrimitiveType() != null) {
                    a = a.toPrimitiveType();
                }
                if (!toType_b.isPrimitive() && b.toPrimitiveType() != null) {
                    b = b.toPrimitiveType();
                }
                return compare(from, a, b);
            }
        }

        return 0;
    }

    private static int compare(TypeDesc from, TypeDesc a, TypeDesc b) {
        if (isConversionPossible(from, a)) {
            if (isConversionPossible(from, b)) {
                if (from.isPrimitive()) {
                    if (a.isPrimitive()) {
                        if (b.isPrimitive()) {
                            // Choose the one with the least amount of widening.
                            return primitiveWidth(a) - primitiveWidth(b);
                        } else {
                            return -1;
                        }
                    } else if (b.isPrimitive()) {
                        return 1;
                    }
                } else {
                    // Choose the one with the shortest distance up the class
                    // hierarchy.
                    Class fromClass = from.toClass();
                    if (!fromClass.isInterface()) {
                        if (a.toClass().isInterface()) {
                            if (!b.toClass().isInterface()) {
                                return -1;
                            }
                        } else if (b.toClass().isInterface()) {
                            return 1;
                        } else {
                            return distance(fromClass, a.toClass())
                                - distance(fromClass, b.toClass());
                        }
                    }
                }
            } else {
                return -1;
            }
        } else if (isConversionPossible(from, b)) {
            return 1;
        }

        return 0;
    }

    // 1 = boolean, 2 = byte, 3 = short, 4 = char, 5 = int, 6 = float, 7 = long, 8 = double
    private static int primitiveWidth(TypeDesc type) {
        switch (type.getTypeCode()) {
        default:
            return 0;
        case TypeDesc.BOOLEAN_CODE:
            return 1;
        case TypeDesc.BYTE_CODE:
            return 2;
        case TypeDesc.SHORT_CODE:
            return 3;
        case TypeDesc.CHAR_CODE:
            return 4;
        case TypeDesc.INT_CODE:
            return 5;
        case TypeDesc.FLOAT_CODE:
            return 6;
        case TypeDesc.LONG_CODE:
            return 7;
        case TypeDesc.DOUBLE_CODE:
            return 8;
        }
    }

    private static int distance(Class from, Class to) {
        int distance = 0;
        while (from != to) {
            from = from.getSuperclass();
            if (from == null) {
                return Integer.MAX_VALUE;
            }
            distance++;
        }
        return distance;
    }
}
