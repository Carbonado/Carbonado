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

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Trigger;

import com.amazon.carbonado.stored.Dummy;

/**
 * Tests for TriggerManager.
 *
 * @author Brian S O'Neill
 */
public class TestTriggerManager extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestTriggerManager.class);
    }

    public TestTriggerManager(String name) {
        super(name);
    }

    @Override
    protected void setUp() {
        beforeTriggers = new ArrayList<TestTrigger>();
        afterTriggers = new ArrayList<TestTrigger>();
        failedTriggers = new ArrayList<TestTrigger>();
    }

    @Override
    protected void tearDown() {
    }

    List<TestTrigger> beforeTriggers;
    List<TestTrigger> afterTriggers;
    List<TestTrigger> failedTriggers;

    public void testAddAndRemove() {
        TriggerManager<Dummy> set = new TriggerManager<Dummy>(null, null);
        Trigger<Dummy> trigger = new TestTrigger<Dummy>();

        assertNull(set.getInsertTrigger());
        assertNull(set.getUpdateTrigger());
        assertNull(set.getDeleteTrigger());

        assertTrue(set.addTrigger(trigger));
        assertNotNull(set.getInsertTrigger());
        assertNotNull(set.getUpdateTrigger());
        assertNotNull(set.getDeleteTrigger());

        assertFalse(set.addTrigger(trigger));
        assertNotNull(set.getInsertTrigger());
        assertNotNull(set.getUpdateTrigger());
        assertNotNull(set.getDeleteTrigger());

        assertTrue(set.removeTrigger(trigger));
        assertNull(set.getInsertTrigger());
        assertNull(set.getUpdateTrigger());
        assertNull(set.getDeleteTrigger());

        assertFalse(set.removeTrigger(trigger));
        assertNull(set.getInsertTrigger());
        assertNull(set.getUpdateTrigger());
        assertNull(set.getDeleteTrigger());

        Trigger<Dummy> trigger2 = new TestTrigger<Dummy>();
        assertTrue(set.addTrigger(trigger));
        assertTrue(set.addTrigger(trigger2));
        assertNotNull(set.getInsertTrigger());
        assertNotNull(set.getUpdateTrigger());
        assertNotNull(set.getDeleteTrigger());

        assertTrue(set.removeTrigger(trigger));
        assertNotNull(set.getInsertTrigger());
        assertNotNull(set.getUpdateTrigger());
        assertNotNull(set.getDeleteTrigger());
        assertTrue(set.removeTrigger(trigger2));
        assertNull(set.getInsertTrigger());
        assertNull(set.getUpdateTrigger());
        assertNull(set.getDeleteTrigger());
    }

    public void testBeforeAndAfterOps() throws Exception {
        TriggerManager<Dummy> set = new TriggerManager<Dummy>(null, null);
        TestTrigger<Dummy> trigger = new TestTrigger<Dummy>();
        set.addTrigger(trigger);
        Dummy d = new Dummy();

        Object state = set.getInsertTrigger().beforeInsert(d);
        assertEquals(1, trigger.beforeInsertCount);
        assertEquals(0, trigger.beforeUpdateCount);
        assertEquals(0, trigger.beforeDeleteCount);

        set.getInsertTrigger().afterInsert(d, state);
        assertEquals(1, trigger.afterInsertCount);
        assertEquals(0, trigger.afterUpdateCount);
        assertEquals(0, trigger.afterDeleteCount);

        state = set.getUpdateTrigger().beforeUpdate(d);
        assertEquals(1, trigger.beforeUpdateCount);
        assertEquals(0, trigger.beforeDeleteCount);

        set.getUpdateTrigger().afterUpdate(d, state);
        assertEquals(1, trigger.afterUpdateCount);
        assertEquals(0, trigger.afterDeleteCount);

        state = set.getDeleteTrigger().beforeDelete(d);
        assertEquals(1, trigger.beforeDeleteCount);

        set.getDeleteTrigger().afterDelete(d, state);
        assertEquals(1, trigger.afterDeleteCount);
    }

    public void testBeforeAndFailedOps() throws Exception {
        TriggerManager<Dummy> set = new TriggerManager<Dummy>(null, null);
        TestTrigger<Dummy> trigger = new TestTrigger<Dummy>();
        set.addTrigger(trigger);
        Dummy d = new Dummy();

        Object state = set.getInsertTrigger().beforeInsert(d);
        assertEquals(1, trigger.beforeInsertCount);
        assertEquals(0, trigger.beforeUpdateCount);
        assertEquals(0, trigger.beforeDeleteCount);

        set.getInsertTrigger().failedInsert(d, state);
        assertEquals(1, trigger.failedInsertCount);
        assertEquals(0, trigger.failedUpdateCount);
        assertEquals(0, trigger.failedDeleteCount);

        state = set.getUpdateTrigger().beforeUpdate(d);
        assertEquals(1, trigger.beforeUpdateCount);
        assertEquals(0, trigger.beforeDeleteCount);

        set.getUpdateTrigger().failedUpdate(d, state);
        assertEquals(1, trigger.failedUpdateCount);
        assertEquals(0, trigger.failedDeleteCount);

        state = set.getDeleteTrigger().beforeDelete(d);
        assertEquals(1, trigger.beforeDeleteCount);

        set.getDeleteTrigger().failedDelete(d, state);
        assertEquals(1, trigger.failedDeleteCount);
    }

    public void testExecutionOrder() throws Exception {
        TriggerManager<Dummy> set = new TriggerManager<Dummy>(null, null);
        TestTrigger<Dummy> trigger = new TestTrigger<Dummy>(null);
        TestTrigger<Dummy> trigger2 = new TestTrigger<Dummy>();
        set.addTrigger(trigger);
        set.addTrigger(trigger2);
        Dummy d = new Dummy();

        // Insert
        {
            Object state = set.getInsertTrigger().beforeInsert(d);
            assertEquals(2, beforeTriggers.size());
            assertEquals(trigger2, beforeTriggers.get(0));
            assertEquals(trigger, beforeTriggers.get(1));

            set.getInsertTrigger().afterInsert(d, state);
            assertEquals(2, afterTriggers.size());
            assertEquals(trigger, afterTriggers.get(0));
            assertEquals(trigger2, afterTriggers.get(1));

            state = set.getInsertTrigger().beforeInsert(d);
            set.getInsertTrigger().failedInsert(d, state);
            assertEquals(2, failedTriggers.size());
            assertEquals(trigger, failedTriggers.get(0));
            assertEquals(trigger2, failedTriggers.get(1));
        }

        beforeTriggers.clear();
        afterTriggers.clear();
        failedTriggers.clear();

        // Update
        {
            Object state = set.getUpdateTrigger().beforeUpdate(d);
            assertEquals(2, beforeTriggers.size());
            assertEquals(trigger2, beforeTriggers.get(0));
            assertEquals(trigger, beforeTriggers.get(1));

            set.getUpdateTrigger().afterUpdate(d, state);
            assertEquals(2, afterTriggers.size());
            assertEquals(trigger, afterTriggers.get(0));
            assertEquals(trigger2, afterTriggers.get(1));

            state = set.getUpdateTrigger().beforeUpdate(d);
            set.getUpdateTrigger().failedUpdate(d, state);
            assertEquals(2, failedTriggers.size());
            assertEquals(trigger, failedTriggers.get(0));
            assertEquals(trigger2, failedTriggers.get(1));
        }

        beforeTriggers.clear();
        afterTriggers.clear();
        failedTriggers.clear();

        // Delete
        {
            Object state = set.getDeleteTrigger().beforeDelete(d);
            assertEquals(2, beforeTriggers.size());
            assertEquals(trigger2, beforeTriggers.get(0));
            assertEquals(trigger, beforeTriggers.get(1));

            set.getDeleteTrigger().afterDelete(d, state);
            assertEquals(2, afterTriggers.size());
            assertEquals(trigger, afterTriggers.get(0));
            assertEquals(trigger2, afterTriggers.get(1));

            state = set.getDeleteTrigger().beforeDelete(d);
            set.getDeleteTrigger().failedDelete(d, state);
            assertEquals(2, failedTriggers.size());
            assertEquals(trigger, failedTriggers.get(0));
            assertEquals(trigger2, failedTriggers.get(1));
        }
    }

    class TestTrigger<S extends Storable> extends Trigger<S> {
        final Object stateObj;

        int beforeInsertCount;
        int afterInsertCount;
        int failedInsertCount;

        int beforeUpdateCount;
        int afterUpdateCount;
        int failedUpdateCount;

        int beforeDeleteCount;
        int afterDeleteCount;
        int failedDeleteCount;

        TestTrigger() {
            this.stateObj = new Object();
        }

        TestTrigger(Object stateObj) {
            this.stateObj = stateObj;
        }

        @Override
        public Object beforeInsert(S storable) {
            beforeInsertCount++;
            beforeTriggers.add(this);
            return stateObj;
        }

        @Override
        public void afterInsert(S storable, Object state) {
            Assert.assertEquals(stateObj, state);
            afterTriggers.add(this);
            afterInsertCount++;
        }

        @Override
        public void failedInsert(S storable, Object state) {
            Assert.assertEquals(stateObj, state);
            failedTriggers.add(this);
            failedInsertCount++;
        }

        @Override
        public Object beforeUpdate(S storable) {
            beforeUpdateCount++;
            beforeTriggers.add(this);
            return stateObj;
        }

        @Override
        public void afterUpdate(S storable, Object state) {
            Assert.assertEquals(stateObj, state);
            afterTriggers.add(this);
            afterUpdateCount++;
        }

        @Override
        public void failedUpdate(S storable, Object state) {
            Assert.assertEquals(stateObj, state);
            failedTriggers.add(this);
            failedUpdateCount++;
        }

        @Override
        public Object beforeDelete(S storable) {
            beforeDeleteCount++;
            beforeTriggers.add(this);
            return stateObj;
        }

        @Override
        public void afterDelete(S storable, Object state) {
            Assert.assertEquals(stateObj, state);
            afterTriggers.add(this);
            afterDeleteCount++;
        }

        @Override
        public void failedDelete(S storable, Object state) {
            Assert.assertEquals(stateObj, state);
            failedTriggers.add(this);
            failedDeleteCount++;
        }

    }
}
