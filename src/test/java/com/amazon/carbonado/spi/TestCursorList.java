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

package com.amazon.carbonado.spi;

import com.amazon.carbonado.Cursor;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.cursor.EmptyCursorFactory;

/**
 * Test case for TransactionManager.CursorList.
 *
 * @author Brian S O'Neill
 */
public class TestCursorList extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestCursorList.class);
    }

    TransactionManager.CursorList mList;

    public TestCursorList(String name) {
        super(name);
    }

    protected void setUp() {
       mList = new TransactionManager.CursorList();
    }

    public void testRegisterFew() {
        assertEquals(0, mList.size());

        {
            Cursor cursor = EmptyCursorFactory.newEmptyCursor();
            mList.register(cursor, null);
            assertEquals(1, mList.size());
            assertEquals(cursor, mList.getCursor(0));
            assertEquals(null, mList.getValue(0));
            Object value = mList.unregister(cursor);
            assertEquals(0, mList.size());
            assertEquals(null, value);
        }

        {
            Cursor cursor_1 = EmptyCursorFactory.newEmptyCursor();
            Cursor cursor_2 = EmptyCursorFactory.newEmptyCursor();
            mList.register(cursor_1, null);
            assertEquals(1, mList.size());
            mList.register(cursor_2, null);
            assertEquals(2, mList.size());
            assertEquals(cursor_1, mList.getCursor(0));
            assertEquals(cursor_2, mList.getCursor(1));
            assertEquals(null, mList.getValue(0));
            assertEquals(null, mList.getValue(1));

            Object value = mList.unregister(cursor_2);
            assertEquals(1, mList.size());
            assertEquals(cursor_1, mList.getCursor(0));
            assertEquals(null, value);
            mList.unregister(cursor_2);
            assertEquals(1, mList.size());
            mList.unregister(cursor_1);
            assertEquals(0, mList.size());
        }

        // unregister in reverse
        {
            Cursor cursor_1 = EmptyCursorFactory.newEmptyCursor();
            Cursor cursor_2 = EmptyCursorFactory.newEmptyCursor();
            mList.register(cursor_1, null);
            mList.register(cursor_2, null);

            mList.unregister(cursor_1);
            assertEquals(1, mList.size());
            assertEquals(cursor_2, mList.getCursor(0));

            mList.unregister(cursor_1);
            assertEquals(1, mList.size());
            mList.unregister(cursor_2);
            assertEquals(0, mList.size());
        }
    }

    public void testRegisterFewValue() {
        Cursor cursor_1 = EmptyCursorFactory.newEmptyCursor();
        Cursor cursor_2 = EmptyCursorFactory.newEmptyCursor();
        String value_1 = "1";
        String value_2 = "2";

        mList.register(cursor_1, value_1);
        assertEquals(1, mList.size());
        assertEquals(cursor_1, mList.getCursor(0));
        assertEquals(value_1, mList.getValue(0));

        mList.register(cursor_2, value_2);
        assertEquals(2, mList.size());
        assertEquals(cursor_1, mList.getCursor(0));
        assertEquals(value_1, mList.getValue(0));
        assertEquals(cursor_2, mList.getCursor(1));
        assertEquals(value_2, mList.getValue(1));

        Object value = mList.unregister(cursor_2);
        assertEquals(1, mList.size());
        assertEquals(cursor_1, mList.getCursor(0));
        assertEquals(value_1, mList.getValue(0));
        assertEquals(value_2, value);

        value = mList.unregister(cursor_2);
        assertEquals(1, mList.size());
        assertEquals(null, value);
        value = mList.unregister(cursor_1);
        assertEquals(0, mList.size());
        assertEquals(value_1, value);
    }

    // Tests that the array expands properly.
    public void testRegisterMany() {
        final int count = 50;
        Cursor[] cursors = new Cursor[count];
        for (int i=0; i<count; i++) {
            cursors[i] = EmptyCursorFactory.newEmptyCursor();
            mList.register(cursors[i], null);
            assertEquals(i + 1, mList.size());
        }

        for (int i=0; i<count; i++) {
            assertEquals(cursors[i], mList.getCursor(i));
            assertEquals(null, mList.getValue(i));
        }

        for (int i=0; i<count; i++) {
            mList.unregister(cursors[i]);
            assertEquals(count - i - 1, mList.size());
        }
    }

    // Tests that the arrays expand properly and store values.
    public void testRegisterManyValues() {
        final int count = 50;
        Cursor[] cursors = new Cursor[count];
        Integer[] values = new Integer[count];
        for (int i=0; i<count; i++) {
            cursors[i] = EmptyCursorFactory.newEmptyCursor();
            values[i] = i;
            mList.register(cursors[i], values[i]);
            assertEquals(i + 1, mList.size());
        }

        for (int i=0; i<count; i++) {
            assertEquals(cursors[i], mList.getCursor(i));
            assertEquals(values[i], mList.getValue(i));
        }

        for (int i=0; i<count; i++) {
            Object value = mList.unregister(cursors[i]);
            assertEquals(count - i - 1, mList.size());
            assertEquals(values[i], value);
        }
    }

    public void testCloseCursors() throws Exception {
        final int count = 50;

        // Without values
        {
            Cursor[] cursors = new Cursor[count];
            for (int i=0; i<count; i++) {
                cursors[i] = EmptyCursorFactory.newEmptyCursor();
                mList.register(cursors[i], null);
            }

            mList.closeCursors();
            assertEquals(0, mList.size());

            /*
            for (int i=0; i<count; i++) {
                assertEquals(true, cursors[i].isClosed());
            }
            */
        }

        // With values
        {
            Cursor[] cursors = new Cursor[count];
            Integer[] values = new Integer[count];
            for (int i=0; i<count; i++) {
                cursors[i] = EmptyCursorFactory.newEmptyCursor();
                values[i] = i;
                mList.register(cursors[i], values[i]);
            }

            mList.closeCursors();
            assertEquals(0, mList.size());

            /*
            for (int i=0; i<count; i++) {
                assertEquals(true, cursors[i].isClosed());
            }
            */
        }
    }
}
