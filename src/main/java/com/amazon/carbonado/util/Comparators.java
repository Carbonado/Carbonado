/*
 * Copyright 2007 Amazon Technologies, Inc. or its affiliates.
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
 * Collection of utility comparators.
 *
 * @author Brian S O'Neill
 */
public class Comparators {
    /**
     * Returns a comparator which can sort single or multi-dimensional arrays
     * of primitves or Comparables.
     *
     * @param unsigned applicable only to arrays of bytes, shorts, ints, or longs
     * @return null if unsupported
     */
    public static <T> Comparator<T> arrayComparator(Class<T> arrayType, boolean unsigned) {
        if (!arrayType.isArray()) {
            throw new IllegalArgumentException();
        }

        Comparator c;

        TypeDesc componentType = TypeDesc.forClass(arrayType.getComponentType());
        switch (componentType.getTypeCode()) {
        case TypeDesc.BYTE_CODE:
            c = unsigned ? UnsignedByteArray : SignedByteArray;
            break;
        case TypeDesc.SHORT_CODE:
            c = unsigned ? UnsignedShortArray : SignedShortArray;
            break;
        case TypeDesc.INT_CODE:
            c = unsigned ? UnsignedIntArray : SignedIntArray;
            break;
        case TypeDesc.LONG_CODE:
            c = unsigned ? UnsignedLongArray : SignedLongArray;
            break;
        case TypeDesc.BOOLEAN_CODE:
            c = BooleanArray;
            break;
        case TypeDesc.CHAR_CODE:
            c = CharArray;
            break;
        case TypeDesc.FLOAT_CODE:
            c = FloatArray;
            break;
        case TypeDesc.DOUBLE_CODE:
            c = DoubleArray;
            break;
        case TypeDesc.OBJECT_CODE: default:
            if (componentType.isArray()) {
                c = new ComparatorArray(arrayComparator(componentType.toClass(), unsigned));
            } else if (Comparable.class.isAssignableFrom(componentType.toClass())) {
                c = ComparableArray;
            } else {
                c = null;
            }
            break;
        }

        return (Comparator<T>) c;
    }

    private static final Comparator<byte[]> SignedByteArray = new Comparator<byte[]>() {
        public int compare(byte[] a, byte[] b) {
            int len = Math.min(a.length, b.length);
            for (int i=0; i<len; i++) {
                byte ai = a[i];
                byte bi = b[i];
                if (ai != bi) {
                    return ai - bi;
                }
            }
            return a.length - b.length;
        }
    };

    private static final Comparator<byte[]> UnsignedByteArray = new Comparator<byte[]>() {
        public int compare(byte[] a, byte[] b) {
            int len = Math.min(a.length, b.length);
            for (int i=0; i<len; i++) {
                byte ai = a[i];
                byte bi = b[i];
                if (ai != bi) {
                    return (ai & 0xff) - (bi & 0xff);
                }
            }
            return a.length - b.length;
        }
    };

    private static final Comparator<short[]> SignedShortArray = new Comparator<short[]>() {
        public int compare(short[] a, short[] b) {
            int len = Math.min(a.length, b.length);
            for (int i=0; i<len; i++) {
                short ai = a[i];
                short bi = b[i];
                if (ai != bi) {
                    return ai - bi;
                }
            }
            return a.length - b.length;
        }
    };

    private static final Comparator<short[]> UnsignedShortArray = new Comparator<short[]>() {
        public int compare(short[] a, short[] b) {
            int len = Math.min(a.length, b.length);
            for (int i=0; i<len; i++) {
                short ai = a[i];
                short bi = b[i];
                if (ai != bi) {
                    return (ai & 0xffff) - (bi & 0xffff);
                }
            }
            return a.length - b.length;
        }
    };

    private static final Comparator<int[]> SignedIntArray = new Comparator<int[]>() {
        public int compare(int[] a, int[] b) {
            int len = Math.min(a.length, b.length);
            for (int i=0; i<len; i++) {
                int ai = a[i];
                int bi = b[i];
                if (ai != bi) {
                    return ai < bi ? -1 : 1;
                }
            }
            return a.length - b.length;
        }
    };

    private static final Comparator<int[]> UnsignedIntArray = new Comparator<int[]>() {
        public int compare(int[] a, int[] b) {
            int len = Math.min(a.length, b.length);
            for (int i=0; i<len; i++) {
                int ai = a[i];
                int bi = b[i];
                if (ai != bi) {
                    return (ai & 0xffffffffL) < (bi & 0xffffffffL) ? -1 : 1;
                }
            }
            return a.length - b.length;
        }
    };

    private static final Comparator<long[]> SignedLongArray = new Comparator<long[]>() {
        public int compare(long[] a, long[] b) {
            int len = Math.min(a.length, b.length);
            for (int i=0; i<len; i++) {
                long ai = a[i];
                long bi = b[i];
                if (ai != bi) {
                    return ai < bi ? -1 : 1;
                }
            }
            return a.length - b.length;
        }
    };

    private static final Comparator<long[]> UnsignedLongArray = new Comparator<long[]>() {
        public int compare(long[] a, long[] b) {
            int len = Math.min(a.length, b.length);
            for (int i=0; i<len; i++) {
                long ai = a[i];
                long bi = b[i];
                if (ai != bi) {
                    long sai = ai >>> 1;
                    long sbi = bi >>> 1;
                    if (sai < sbi) {
                        return -1;
                    }
                    if (sai > sbi) {
                        return 1;
                    }
                    return (ai & 1) == 0 ? -1 : 1;
                }
            }
            return a.length - b.length;
        }
    };

    private static final Comparator<boolean[]> BooleanArray = new Comparator<boolean[]>() {
        public int compare(boolean[] a, boolean[] b) {
            int len = Math.min(a.length, b.length);
            for (int i=0; i<len; i++) {
                boolean ai = a[i];
                boolean bi = b[i];
                if (ai != bi) {
                    return ai ? 1 : -1;
                }
            }
            return a.length - b.length;
        }
    };

    private static final Comparator<char[]> CharArray = new Comparator<char[]>() {
        public int compare(char[] a, char[] b) {
            int len = Math.min(a.length, b.length);
            for (int i=0; i<len; i++) {
                char ai = a[i];
                char bi = b[i];
                if (ai != bi) {
                    return ai - bi;
                }
            }
            return a.length - b.length;
        }
    };

    private static final Comparator<float[]> FloatArray = new Comparator<float[]>() {
        public int compare(float[] a, float[] b) {
            int len = Math.min(a.length, b.length);
            for (int i=0; i<len; i++) {
                float ai = a[i];
                float bi = b[i];
                if (ai != bi) {
                    return Float.compare(ai, bi);
                }
            }
            return a.length - b.length;
        }
    };

    private static final Comparator<double[]> DoubleArray = new Comparator<double[]>() {
        public int compare(double[] a, double[] b) {
            int len = Math.min(a.length, b.length);
            for (int i=0; i<len; i++) {
                double ai = a[i];
                double bi = b[i];
                if (ai != bi) {
                    return Double.compare(ai, bi);
                }
            }
            return a.length - b.length;
        }
    };

    private static final Comparator<Comparable[]> ComparableArray = new Comparator<Comparable[]>()
    {
        public int compare(Comparable[] a, Comparable[] b) {
            int len = Math.min(a.length, b.length);
            for (int i=0; i<len; i++) {
                Comparable ai = a[i];
                Comparable bi = b[i];
                if (ai == bi) {
                    continue;
                }
                if (ai == null) {
                    return 1;
                }
                if (bi == null) {
                    return -1;
                }
                int compare = ai.compareTo(bi);
                if (compare != 0) {
                    return compare;
                }
            }
            return a.length - b.length;
        }
    };

    private static class ComparatorArray implements Comparator<Object[]> {
        private final Comparator<Object> mComparator;

        ComparatorArray(Comparator<Object> comparator) {
            mComparator = comparator;
        }

        public int compare(Object[] a, Object[] b) {
            int len = Math.min(a.length, b.length);
            for (int i=0; i<len; i++) {
                Object ai = a[i];
                Object bi = b[i];
                if (ai == bi) {
                    continue;
                }
                if (ai == null) {
                    return 1;
                }
                if (bi == null) {
                    return -1;
                }
                int compare = mComparator.compare(ai, bi);
                if (compare != 0) {
                    return compare;
                }
            }
            return a.length - b.length;
        }
    }
}
