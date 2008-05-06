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

import java.lang.annotation.Annotation;

/**
 * Prints an Annotation into a parseable format, exactly the same as Java
 * Annotation syntax.
 *
 * @author Brian S O'Neill
 */
public class AnnotationPrinter extends AnnotationVisitor<Object, Object> {
    private final StringBuilder mBuilder;

    /**
     * @param sort when true, sort annotation members by name (case sensitive)
     * @param b StringBuilder to get printed results
     */
    public AnnotationPrinter(boolean sort, StringBuilder b) {
        super(sort);
        mBuilder = b;
    }

    /**
     * Prints the annotation to the builder passed to the constructor.
     *
     * @param value Annotation to visit
     * @return null
     */
    public Object visit(Annotation value) {
        return visit(value, null);
    }

    @Override
    public Object visit(String name, int pos, Annotation value, Object param) {
        appendName(name, pos);
        mBuilder.append('@');
        mBuilder.append(value.annotationType().getName());
        mBuilder.append('(');
        super.visit(name, pos, value, param);
        mBuilder.append(')');
        return null;
    }

    @Override
    public Object visit(String name, int pos, int value, Object param) {
        appendName(name, pos);
        mBuilder.append(value);
        return null;
    }

    @Override
    public Object visit(String name, int pos, long value, Object param) {
        appendName(name, pos);
        mBuilder.append(value);
        mBuilder.append('L');
        return null;
    }

    @Override
    public Object visit(String name, int pos, float value, Object param) {
        appendName(name, pos);
        if (Float.isNaN(value)) {
            mBuilder.append("java.lang.Float.NaN");
        } else if (value == Float.POSITIVE_INFINITY) {
            mBuilder.append("java.lang.Float.POSITIVE_INFINITY");
        } else if (value == Float.NEGATIVE_INFINITY) {
            mBuilder.append("java.lang.Float.NEGATIVE_INFINITY");
        } else {
            mBuilder.append(value);
            mBuilder.append('f');
        }
        return null;
    }

    @Override
    public Object visit(String name, int pos, double value, Object param) {
        appendName(name, pos);
        if (Double.isNaN(value)) {
            mBuilder.append("java.lang.Double.NaN");
        } else if (value == Double.POSITIVE_INFINITY) {
            mBuilder.append("java.lang.Double.POSITIVE_INFINITY");
        } else if (value == Double.NEGATIVE_INFINITY) {
            mBuilder.append("java.lang.Double.NEGATIVE_INFINITY");
        } else {
            mBuilder.append(value);
            mBuilder.append('d');
        }
        return null;
    }

    @Override
    public Object visit(String name, int pos, boolean value, Object param) {
        appendName(name, pos);
        mBuilder.append(value);
        return null;
    }

    @Override
    public Object visit(String name, int pos, byte value, Object param) {
        appendName(name, pos);
        mBuilder.append(value);
        return null;
    }

    @Override
    public Object visit(String name, int pos, short value, Object param) {
        appendName(name, pos);
        mBuilder.append(value);
        return null;
    }

    @Override
    public Object visit(String name, int pos, char value, Object param) {
        appendName(name, pos);
        mBuilder.append('\'');
        appendChar(value, false);
        mBuilder.append('\'');
        return null;
    }

    private void appendChar(char c, boolean forString) {
        switch (c) {
        case '\b':
        case '\t':
        case '\n':
        case '\f':
        case '\r':
        case '\"':
        case '\'':
        case '\\':
            if (forString) {
                if (c != '\'') {
                    mBuilder.append('\\');
                }
            } else if (c != '\"') {
                mBuilder.append('\\');
            }

            char e = c;

            switch (c) {
            case '\b':
                e = 'b';
                break;
            case '\t':
                e = 't';
                break;
            case '\n':
                e = 'n';
                break;
            case '\f':
                e = 'f';
                break;
            case '\r':
                e = 'r';
                break;
            }

            mBuilder.append(e);
            break;

        default:
            if (c >= 32 && c <= 126) {
                if (forString) {
                    if (c == '"') {
                        mBuilder.append('\\');
                    }
                } else {
                    if (c == '\'') {
                        mBuilder.append('\\');
                    }
                }
                mBuilder.append(c);
                break;
            }

            mBuilder.append('\\');
            mBuilder.append('u');
            if (c < 0x1000) {
                mBuilder.append('0');
                if (c < 0x100) {
                    mBuilder.append('0');
                    if (c < 0x10) {
                        mBuilder.append('0');
                    }
                }
            }
            mBuilder.append(Integer.toHexString(c));
        }
    }

    @Override
    public Object visit(String name, int pos, String value, Object param) {
        appendName(name, pos);
        mBuilder.append('"');
        int length = value.length();
        for (int i=0; i<length; i++) {
            appendChar(value.charAt(i), true);
        }
        mBuilder.append('"');
        return null;
    }

    @Override
    public Object visit(String name, int pos, Class value, Object param) {
        appendName(name, pos);
        mBuilder.append(value.getName());
        mBuilder.append(".class");
        return null;
    }

    @Override
    public Object visit(String name, int pos, Enum value, Object param) {
        appendName(name, pos);
        mBuilder.append(value.getDeclaringClass().getName());
        mBuilder.append('.');
        mBuilder.append(value.name());
        return null;
    }

    @Override
    public Object visit(String name, int pos, Annotation[] value, Object param) {
        appendName(name, pos);
        mBuilder.append('{');
        super.visit(name, pos, value, param);
        mBuilder.append('}');
        return null;
    }

    @Override
    public Object visit(String name, int pos, int[] value, Object param) {
        appendName(name, pos);
        mBuilder.append('{');
        super.visit(name, pos, value, param);
        mBuilder.append('}');
        return null;
    }

    @Override
    public Object visit(String name, int pos, long[] value, Object param) {
        appendName(name, pos);
        super.visit(name, pos, value, param);
        mBuilder.append('}');
        return null;
    }

    @Override
    public Object visit(String name, int pos, float[] value, Object param) {
        appendName(name, pos);
        mBuilder.append('{');
        super.visit(name, pos, value, param);
        mBuilder.append('}');
        return null;
    }

    @Override
    public Object visit(String name, int pos, double[] value, Object param) {
        appendName(name, pos);
        mBuilder.append('{');
        super.visit(name, pos, value, param);
        mBuilder.append('}');
        return null;
    }

    @Override
    public Object visit(String name, int pos, boolean[] value, Object param) {
        appendName(name, pos);
        mBuilder.append('{');
        super.visit(name, pos, value, param);
        mBuilder.append('}');
        return null;
    }

    @Override
    public Object visit(String name, int pos, byte[] value, Object param) {
        appendName(name, pos);
        mBuilder.append('{');
        super.visit(name, pos, value, param);
        mBuilder.append('}');
        return null;
    }

    @Override
    public Object visit(String name, int pos, short[] value, Object param) {
        appendName(name, pos);
        mBuilder.append('{');
        super.visit(name, pos, value, param);
        mBuilder.append('}');
        return null;
    }

    @Override
    public Object visit(String name, int pos, char[] value, Object param) {
        appendName(name, pos);
        mBuilder.append('{');
        super.visit(name, pos, value, param);
        mBuilder.append('}');
        return null;
    }

    @Override
    public Object visit(String name, int pos, String[] value, Object param) {
        appendName(name, pos);
        mBuilder.append('{');
        super.visit(name, pos, value, param);
        mBuilder.append('}');
        return null;
    }

    @Override
    public Object visit(String name, int pos, Class[] value, Object param) {
        appendName(name, pos);
        mBuilder.append('{');
        super.visit(name, pos, value, param);
        mBuilder.append('}');
        return null;
    }

    @Override
    public Object visit(String name, int pos, Enum[] value, Object param) {
        appendName(name, pos);
        mBuilder.append('{');
        super.visit(name, pos, value, param);
        mBuilder.append('}');
        return null;
    }

    private void appendName(String name, int pos) {
        if (pos > 0) {
            mBuilder.append(", ");
        }
        if (name != null) {
            mBuilder.append(name);
            mBuilder.append('=');
        }
    }
}
