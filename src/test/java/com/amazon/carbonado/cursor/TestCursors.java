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

package com.amazon.carbonado.cursor;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;

import com.amazon.carbonado.stored.Dummy;
import com.amazon.carbonado.stored.StorableTestMinimal;

/**
 *
 * @author Brian S O'Neill
 */
public class TestCursors extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestCursors.class);
    }

    public TestCursors(String name) {
        super(name);
    }

    protected void setUp() {
    }

    protected void tearDown() {
    }

    public void testSingleton() throws Exception {
        try {
            new SingletonCursor<Object>(null);
            fail();
        } catch (IllegalArgumentException e) {
        }

        Cursor<String> cursor = new SingletonCursor<String>("hello");

        assertTrue(cursor.hasNext());
        assertEquals("hello", cursor.next());
        assertFalse(cursor.hasNext());
        try {
            cursor.next();
            fail();
        } catch (NoSuchElementException e) {
        }

        assertEquals(0, cursor.skipNext(1));

        cursor = new SingletonCursor<String>("world");
        List<String> list = cursor.toList(0);
        assertEquals(0, list.size());
        list = cursor.toList(10);
        assertEquals(1, list.size());
        assertEquals("world", list.get(0));

        cursor = new SingletonCursor<String>("world");
        cursor.close();
        assertFalse(cursor.hasNext());

        cursor = new SingletonCursor<String>("world");
        assertEquals(1, cursor.skipNext(2));
        assertEquals(0, cursor.skipNext(1));
        assertFalse(cursor.hasNext());
    }

    public void testUnion() throws Exception {
        Cursor<Element> left, right, union;

        // Two empty sets.
        left  = createElements();
        right = createElements();
        union = new UnionCursor<Element>(left, right, new ElementComparator());

        compareElements(union);

        // Right set empty.
        left  = createElements(1, 2, 3, 4);
        right = createElements();
        union = new UnionCursor<Element>(left, right, new ElementComparator());

        compareElements(union, 1, 2, 3, 4);

        // Left set empty.
        left  = createElements();
        right = createElements(3, 4, 5, 6);
        union = new UnionCursor<Element>(left, right, new ElementComparator());

        compareElements(union, 3, 4, 5, 6);

        // No overlap.
        left  = createElements(1, 2, 3         );
        right = createElements(         4, 5, 6);
        union = new UnionCursor<Element>(left, right, new ElementComparator());

        compareElements(union, 1, 2, 3, 4, 5, 6);

        // Overlap.
        left  = createElements(1, 2, 3, 4      );
        right = createElements(      3, 4, 5, 6);
        union = new UnionCursor<Element>(left, right, new ElementComparator());

        compareElements(union, 1, 2, 3, 4, 5, 6);

        // Swapped overlap.
        left  = createElements(      3, 4, 5, 6);
        right = createElements(1, 2, 3, 4      );
        union = new UnionCursor<Element>(left, right, new ElementComparator());

        compareElements(union, 1, 2, 3, 4, 5, 6);

        // Equivalent.
        left  = createElements(1, 2, 3, 4, 5, 6);
        right = createElements(1, 2, 3, 4, 5, 6);
        union = new UnionCursor<Element>(left, right, new ElementComparator());

        compareElements(union, 1, 2, 3, 4, 5, 6);

        // Complex.
        left  = createElements(1, 2, 3,    5, 6, 7,           11, 12, 13,             17, 18, 19);
        right = createElements(1, 2,    4, 5, 6,       9, 10,         13, 14,         17, 18    );
        union = new UnionCursor<Element>(left, right, new ElementComparator());

        compareElements(union, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 17, 18, 19);

        // Dups.
        left  = createElements(1,             2, 2,    3, 3, 3, 3         );
        right = createElements(1, 1, 1, 1, 1, 2, 2, 2,             4, 4, 4);
        union = new UnionCursor<Element>(left, right, new ElementComparator());

        compareElements(union, 1, 1, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4);
    }

    public void testIntersection() throws Exception {
        Cursor<Element> left, right, inter;

        // Two empty sets.
        left  = createElements();
        right = createElements();
        inter = new IntersectionCursor<Element>(left, right, new ElementComparator());

        compareElements(inter);

        // Right set empty.
        left  = createElements(1, 2, 3, 4);
        right = createElements();
        inter = new IntersectionCursor<Element>(left, right, new ElementComparator());

        compareElements(inter);

        // Left set empty.
        left  = createElements();
        right = createElements(3, 4, 5, 6);
        inter = new IntersectionCursor<Element>(left, right, new ElementComparator());

        compareElements(inter);

        // No overlap.
        left  = createElements(1, 2, 3         );
        right = createElements(         4, 5, 6);
        inter = new IntersectionCursor<Element>(left, right, new ElementComparator());

        compareElements(inter);

        // Overlap.
        left  = createElements(1, 2, 3, 4      );
        right = createElements(      3, 4, 5, 6);
        inter = new IntersectionCursor<Element>(left, right, new ElementComparator());

        compareElements(inter, 3, 4);

        // Swapped overlap.
        left  = createElements(      3, 4, 5, 6);
        right = createElements(1, 2, 3, 4      );
        inter = new IntersectionCursor<Element>(left, right, new ElementComparator());

        compareElements(inter, 3, 4);

        // Equivalent.
        left  = createElements(1, 2, 3, 4, 5, 6);
        right = createElements(1, 2, 3, 4, 5, 6);
        inter = new IntersectionCursor<Element>(left, right, new ElementComparator());

        compareElements(inter, 1, 2, 3, 4, 5, 6);

        // Complex.
        left  = createElements(1, 2, 3,    5, 6, 7,           11, 12, 13,             17, 18, 19);
        right = createElements(1, 2,    4, 5, 6,       9, 10,         13, 14,         17, 18    );
        inter = new IntersectionCursor<Element>(left, right, new ElementComparator());

        compareElements(inter, 1, 2, 5, 6, 13, 17, 18);

        // Dups.
        left  = createElements(1,             2, 2,    3, 3, 3, 3         );
        right = createElements(1, 1, 1, 1, 1, 2, 2, 2,             4, 4, 4);
        inter = new IntersectionCursor<Element>(left, right, new ElementComparator());

        compareElements(inter, 1, 2, 2);
    }

    public void testDifference() throws Exception {
        Cursor<Element> left, right, diff;

        // Two empty sets.
        left  = createElements();
        right = createElements();
        diff  = new DifferenceCursor<Element>(left, right, new ElementComparator());

        compareElements(diff);

        // Right set empty.
        left  = createElements(1, 2, 3, 4);
        right = createElements();
        diff  = new DifferenceCursor<Element>(left, right, new ElementComparator());

        compareElements(diff, 1, 2, 3, 4);

        // Left set empty.
        left  = createElements();
        right = createElements(3, 4, 5, 6);
        diff  = new DifferenceCursor<Element>(left, right, new ElementComparator());

        compareElements(diff);

        // No overlap.
        left  = createElements(1, 2, 3         );
        right = createElements(         4, 5, 6);
        diff  = new DifferenceCursor<Element>(left, right, new ElementComparator());

        compareElements(diff, 1, 2, 3);

        // Overlap.
        left  = createElements(1, 2, 3, 4      );
        right = createElements(      3, 4, 5, 6);
        diff  = new DifferenceCursor<Element>(left, right, new ElementComparator());

        compareElements(diff, 1, 2);

        // Swapped overlap.
        left  = createElements(      3, 4, 5, 6);
        right = createElements(1, 2, 3, 4      );
        diff  = new DifferenceCursor<Element>(left, right, new ElementComparator());

        compareElements(diff, 5, 6);

        // Equivalent.
        left  = createElements(1, 2, 3, 4, 5, 6);
        right = createElements(1, 2, 3, 4, 5, 6);
        diff  = new DifferenceCursor<Element>(left, right, new ElementComparator());

        compareElements(diff);

        // Complex.
        left  = createElements(1, 2, 3,    5, 6, 7,           11, 12, 13,             17, 18, 19);
        right = createElements(1, 2,    4, 5, 6,       9, 10,         13, 14,         17, 18    );
        diff  = new DifferenceCursor<Element>(left, right, new ElementComparator());

        compareElements(diff, 3, 7, 11, 12, 19);

        // Dups.
        left  = createElements(1,             2, 2,    3, 3, 3, 3         );
        right = createElements(1, 1, 1, 1, 1, 2, 2, 2,             4, 4, 4);
        diff  = new DifferenceCursor<Element>(left, right, new ElementComparator());

        compareElements(diff, 3, 3, 3, 3);
    }

    public void testSymmetricDifference() throws Exception {
        Cursor<Element> left, right, diff;

        // Two empty sets.
        left  = createElements();
        right = createElements();
        diff  = new SymmetricDifferenceCursor<Element>(left, right, new ElementComparator());

        compareElements(diff);

        // Right set empty.
        left  = createElements(1, 2, 3, 4);
        right = createElements();
        diff  = new SymmetricDifferenceCursor<Element>(left, right, new ElementComparator());

        compareElements(diff, 1, 2, 3, 4);

        // Left set empty.
        left  = createElements();
        right = createElements(3, 4, 5, 6);
        diff  = new SymmetricDifferenceCursor<Element>(left, right, new ElementComparator());

        compareElements(diff, 3, 4, 5, 6);

        // No overlap.
        left  = createElements(1, 2, 3         );
        right = createElements(         4, 5, 6);
        diff  = new SymmetricDifferenceCursor<Element>(left, right, new ElementComparator());

        compareElements(diff, 1, 2, 3, 4, 5, 6);

        // Overlap.
        left  = createElements(1, 2, 3, 4      );
        right = createElements(      3, 4, 5, 6);
        diff  = new SymmetricDifferenceCursor<Element>(left, right, new ElementComparator());

        compareElements(diff, 1, 2, 5, 6);

        // Swapped overlap.
        left  = createElements(      3, 4, 5, 6);
        right = createElements(1, 2, 3, 4      );
        diff  = new SymmetricDifferenceCursor<Element>(left, right, new ElementComparator());

        compareElements(diff, 1, 2, 5, 6);

        // Equivalent.
        left  = createElements(1, 2, 3, 4, 5, 6);
        right = createElements(1, 2, 3, 4, 5, 6);
        diff  = new SymmetricDifferenceCursor<Element>(left, right, new ElementComparator());

        compareElements(diff);

        // Complex.
        left  = createElements(1, 2, 3,    5, 6, 7,           11, 12, 13,             17, 18, 19);
        right = createElements(1, 2,    4, 5, 6,       9, 10,         13, 14,         17, 18    );
        diff  = new SymmetricDifferenceCursor<Element>(left, right, new ElementComparator());

        compareElements(diff, 3, 4, 7, 9, 10, 11, 12, 14, 19);

        // Dups.
        left  = createElements(1,             2, 2,    3, 3, 3, 3         );
        right = createElements(1, 1, 1, 1, 1, 2, 2, 2,             4, 4, 4);
        diff  = new SymmetricDifferenceCursor<Element>(left, right, new ElementComparator());

        compareElements(diff, 1, 1, 1, 1, 2, 3, 3, 3, 3, 4, 4, 4);
    }

    private Cursor<Element> createElements(int... ids) {
        Arrays.sort(ids);
        Element[] elements = new Element[ids.length];
        for (int i=0; i<ids.length; i++) {
            elements[i] = new Element(ids[i]);
        }
        return new IteratorCursor<Element>(Arrays.asList(elements));
    }

    private void compareElements(Cursor<Element> elements, int... expectedIDs)
        throws FetchException
    {
        for (int id : expectedIDs) {
            if (elements.hasNext()) {
                Element e = elements.next();
                if (e.getId() != id) {
                    fail("Element mismatch: expected=" + id + ", actual=" + e.getId());
                    elements.close();
                    return;
                }
            } else {
                fail("Too few elements in cursor");
                return;
            }
        }

        if (elements.hasNext()) {
            Element e = elements.next();
            fail("Too many elements in cursor: " + e.getId());
            elements.close();
        }
    }

    private static class Element extends Dummy implements StorableTestMinimal {
        private int mID;

        Element(int id) {
            mID = id;
        }

        public int getId() {
            return mID;
        }

        public void setId(int id) {
            mID = id;
        }

        public String toString() {
            return "element " + mID;
        }
    }

    private static class ElementComparator implements Comparator<Element> {
        public int compare(Element a, Element b) {
            int ai = a.getId();
            int bi = b.getId();
            if (ai < bi) {
                return -1;
            } else if (ai > bi) {
                return 1;
            }
            return 0;
        }
    }
}
