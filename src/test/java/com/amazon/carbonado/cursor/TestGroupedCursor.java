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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;

/**
 *
 *
 * @author Brian S O'Neill
 */
public class TestGroupedCursor extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestGroupedCursor.class);
    }

    public TestGroupedCursor(String name) {
        super(name);
    }

    protected void setUp() {
    }

    protected void tearDown() {
    }

    public void testGrouping() throws Exception {
        List<Triple> triples = new ArrayList<Triple>();
        triples.add(new Triple(1, 1, 1));
        triples.add(new Triple(1, 1, 2));
        triples.add(new Triple(1, 1, 3));
        triples.add(new Triple(1, 2, 5));
        triples.add(new Triple(1, 2, 9));
        triples.add(new Triple(3, 1, 10));
        triples.add(new Triple(3, 2, 16));
        triples.add(new Triple(3, 2, 100));

        // Should already be sorted, but no harm done
        Collections.sort(triples);

        Cursor<Pair> cursor = new GroupedCursor<Triple, Pair>
            (new IteratorCursor<Triple>(triples), Triple.class, "a", "b")
        {
            private Pair aggregate;

            protected void beginGroup(Triple groupLeader) {
                aggregate = new Pair(groupLeader.getA(), groupLeader.getB());
                aggregate.add(groupLeader.getC());
            }

            protected void addToGroup(Triple groupMember) {
                aggregate.add(groupMember.getC());
            }

            protected Pair finishGroup() {
                return aggregate;
            }
        };

        List<Pair> pairs = new ArrayList<Pair>();

        while (cursor.hasNext()) {
            pairs.add(cursor.next());
        }

        assertEquals(4, pairs.size());

        assertEquals(1, pairs.get(0).getA());
        assertEquals(1, pairs.get(0).getB());
        assertEquals(6, pairs.get(0).sum);

        assertEquals(1, pairs.get(1).getA());
        assertEquals(2, pairs.get(1).getB());
        assertEquals(14, pairs.get(1).sum);

        assertEquals(3, pairs.get(2).getA());
        assertEquals(1, pairs.get(2).getB());
        assertEquals(10, pairs.get(2).sum);

        assertEquals(3, pairs.get(3).getA());
        assertEquals(2, pairs.get(3).getB());
        assertEquals(116, pairs.get(3).sum);
    }

    static int compare(int x, int y) {
        if (x < y) {
            return -1;
        } else if (x > y) {
            return 1;
        }
        return 0;
    }

    public static class Pair implements Comparable {
        final int a;
        final int b;

        int sum;

        Pair(int a, int b) {
            this.a = a;
            this.b = b;
        }

        public int getA() {
            return a;
        }

        public int getB() {
            return b;
        }

        public void add(int x) {
            sum += x;
        }

        public int compareTo(Object obj) {
            Pair other = (Pair) obj;
            int result = compare(a, other.a);
            if (result == 0) {
                result = compare(b, other.b);
            }
            return result;
        }

        public String toString() {
            return "a=" + a + ", b=" + b + ", sum=" + sum;
        }
    }

    public static class Triple extends Pair {
        final int c;

        Triple(int a, int b, int c) {
            super(a, b);
            this.c = c;
        }

        public int getC() {
            return c;
        }

        public int compareTo(Object obj) {
            int result = super.compareTo(obj);
            if (result == 0) {
                Triple other = (Triple) obj;
                result = compare(c, other.c);
            }
            return result;
        }

        public String toString() {
            return "a=" + a + ", b=" + b + ", c=" + c;
        }
    }
}
