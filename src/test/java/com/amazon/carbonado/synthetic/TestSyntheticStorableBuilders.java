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

package com.amazon.carbonado.synthetic;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.joda.time.DateTime;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.PrimaryKey;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.Version;
import com.amazon.carbonado.Nullable;
import com.amazon.carbonado.Join;

import com.amazon.carbonado.adapter.YesNoAdapter;
import com.amazon.carbonado.adapter.TextAdapter;
import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.info.StorableInfo;
import com.amazon.carbonado.info.StorableIntrospector;
import com.amazon.carbonado.info.StorableProperty;

import com.amazon.carbonado.repo.toy.ToyRepository;

import com.amazon.carbonado.stored.StorableTestBasic;

/**
 * @author Don Schneider
 */
public class TestSyntheticStorableBuilders extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestSyntheticStorableBuilders.class);
    }

    private static final ValueVendor VENDOR = new ValueVendor();

    public static final String VERSION = '@'
            + Version.class.toString().substring(10) + "()";

    public static final String NULLABLE = '@'
            + Nullable.class.toString().substring(10) + "()";

    public static final String JOIN = '@' + Join.class.toString().substring(10) + '(';

    public static final String INTERNAL = "internal=[]";

    public static final String EXTERNAL = "external=[]";

    public final static String YESNO = '@'
            + YesNoAdapter.class.toString().substring(10) + "(lenient=true)";

    public static final String TEXT = '@'
            + TextAdapter.class.toString().substring(10) + "(charset=UTF-8, altCharsets=[])";

    public static final String TEXT_2 = '@'
            + TextAdapter.class.toString().substring(10) + "(altCharsets=[], charset=UTF-8)";

    private static Set s_storableInterfaceMethods = new HashSet();

    public static final TestDef[] TESTS = new TestDef[] {
            new TestDef("string/long",
                        StorableTestBasic.class,
                        StorableTestBasic.class.getName() + "~N+stringProp-longProp",
                        new IndexDef[] {
                                new IndexDef("stringProp", Direction.ASCENDING),
                                new IndexDef("longProp", Direction.DESCENDING), },
                        new MethodDef[] {
                                new MethodDef("getStringProp", new String[] { TEXT }),
                                new MethodDef("getLongProp", null),
                                new MethodDef("getId", null),
                                new MethodDef("getIntProp", null),
                                new MethodDef("getDoubleProp", null),
                                // FIXME: add version prop to STB
//                                new MethodDef("getVersionNumber",
//                                              new String[] { VERSION }),
                                new MethodDef("getMaster_0", new String[] {
                                        NULLABLE, JOIN }),
                                new MethodDef("getIsConsistent_0", null),
//                                new MethodDef("setVersionNumber", null),
                                new MethodDef("setAllProperties_0", null),
                                new MethodDef("setMaster_0", null),
                                new MethodDef("setId", null),

                                new MethodDef("setStringProp", null),
                                new MethodDef("setLongProp", null),
                                new MethodDef("setID", null),
                                new MethodDef("setIntProp", null),
                                new MethodDef("setDoubleProp", null),

                                new MethodDef("getClass", null),
                                new MethodDef("wait", null),
                                new MethodDef("notify", null),
                                new MethodDef("notifyAll", null), },
                        null),
            new TestDef("string/long+synthetic",
                        StorableTestBasic.class,
                        StorableTestBasic.class.getName() + "~N+stringProp-longProp",
                        new IndexDef[] {
                                new IndexDef("stringProp", Direction.ASCENDING),
                                new IndexDef("longProp", Direction.DESCENDING), },
                        new MethodDef[] {
                                new MethodDef("getStringProp", new String[] { TEXT }),
                                new MethodDef("getLongProp", null),
                                new MethodDef("getId", null),
                                new MethodDef("getIntProp", null),
                                new MethodDef("getDoubleProp", null),
                                // FIXME: add version prop to STB
//                                new MethodDef("getVersionNumber",
//                                              new String[] { VERSION }),
                                new MethodDef("getMaster_0", new String[] {
                                        NULLABLE, JOIN }),
                                new MethodDef("getIsConsistent_0", null),
//                                new MethodDef("setVersionNumber", null),
                                new MethodDef("setAllProperties_0", null),
                                new MethodDef("setMaster_0", null),
                                new MethodDef("setId", null),

                                new MethodDef("setStringProp", null),
                                new MethodDef("setLongProp", null),
                                new MethodDef("setID", null),
                                new MethodDef("setIntProp", null),
                                new MethodDef("setDoubleProp", null),

                                new MethodDef("getClass", null),
                                new MethodDef("wait", null),
                                new MethodDef("notify", null),
                                new MethodDef("notifyAll", null),

                                new MethodDef("getTestSynth", new String[] { NULLABLE }),
                                new MethodDef("setTestSynth", null),
                        },
                        new SyntheticProperty[] { new SyntheticProperty("testSynth",
                                                                        String.class,
                                                                        true,
                                                                        false) }),
            new TestDef("string/long+versionedSynthetic",
                        StorableTestBasic.class,
                        StorableTestBasic.class.getName() + "~N+stringProp-longProp",
                        new IndexDef[] {
                                new IndexDef("stringProp", Direction.ASCENDING),
                                new IndexDef("longProp", Direction.DESCENDING), },
                        new MethodDef[] {

                                new MethodDef("getStringProp", new String[] { TEXT }),
                                new MethodDef("getLongProp", null),
                                new MethodDef("getId", null),
                                new MethodDef("getIntProp", null),
                                new MethodDef("getDoubleProp", null),
                                new MethodDef("getMaster_0", new String[] {
                                        NULLABLE, JOIN }),
                                new MethodDef("getIsConsistent_0", null),
                                new MethodDef("setAllProperties_0", null),
                                new MethodDef("setMaster_0", null),
                                new MethodDef("setId", null),

                                new MethodDef("setStringProp", null),
                                new MethodDef("setLongProp", null),
                                new MethodDef("setID", null),
                                new MethodDef("setIntProp", null),
                                new MethodDef("setDoubleProp", null),

                                new MethodDef("getClass", null),
                                new MethodDef("wait", null),
                                new MethodDef("notify", null),
                                new MethodDef("notifyAll", null),

                                new MethodDef("getTestSynth", new String[] { VERSION  }),
                                new MethodDef("setTestSynth", null),
                        },
                        new SyntheticProperty[] { new SyntheticProperty("testSynth",
                                                                        int.class,
                                                                        false,
                                                                        true) }) };

    static {
        for (Method m : Storable.class.getMethods()) {
            s_storableInterfaceMethods.add(m.getName());
        }
    }

    protected Repository mRepository;

    public TestSyntheticStorableBuilders(String name) {
        super(name);
    }

    /* (non-Javadoc)
     * @see junit.framework.TestCase#setUp()
     */
    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        mRepository = new ToyRepository();
    }

    @Override
    protected void tearDown() throws Exception {
        if (mRepository != null) {
            mRepository.close();
            mRepository = null;
        }
    }

    public void testSanity() throws Exception {
        exerciseStorable(StorableTestBasic.class);
    }


    /**
     * Test syntheticBuilder
     */
    public <S extends Storable> void test_syntheticBuilder() throws Exception {
        SyntheticProperty[] props = new SyntheticProperty[] {
                new SyntheticProperty("synthId",
                                      int.class,
                                      false,
                                      false),
                new SyntheticProperty("synthStr",
                                      String.class,
                                      true,
                                      false),
                new SyntheticProperty("synthInt",
                                      int.class,
                                      false,
                                      false),
                new SyntheticProperty("synthVers",
                                      int.class,
                                      false,
                                      true),
                new SyntheticProperty("synthBool",
                                      boolean.class,
                                      false,
                                      false), };

        TestDef test = new TestDef("synthetic",
                                   null,
                                   "TSSB~synthId~synthStr~synthInt~synthVers~synthBool",
                                   null,
                                   new MethodDef[] {
                                           new MethodDef("getSynthId",
                                                         null),
                                           new MethodDef("getSynthStr",
                                                         new String[] { NULLABLE }),
                                           new MethodDef("getSynthInt", null),
                                           new MethodDef("getSynthVers",
                                                         new String[] { VERSION }),
                                           new MethodDef("isSynthBool", null),

                                           new MethodDef("setSynthId", null),
                                           new MethodDef("setSynthStr", null),
                                           new MethodDef("setSynthInt", null),
                                           new MethodDef("setSynthVers", null),
                                           new MethodDef("setSynthBool", null),

                                           new MethodDef("getClass", null),
                                           new MethodDef("wait", null),
                                           new MethodDef("notify", null),
                                           new MethodDef("notifyAll", null), },
                                   props);

        SyntheticStorableBuilder b = new SyntheticStorableBuilder(
                                               "TSSB", this.getClass().getClassLoader());
        int i = 0;
        for (SyntheticProperty source : props) {
            b.addProperty(source);
            if (i == 0) {
                // First is primary key.
                b.addPrimaryKey().addProperty(source.getName());
            }
            i++;
        }
        Class synth = b.build();
        validateIndexEntry(test, synth);

        exerciseStorable(synth);
    }



    public void testSyntheticReference() throws Exception {
        for (TestDef test : TESTS) {
            StorableInfo info = StorableIntrospector.examine(test.mClass);

            SyntheticStorableReferenceBuilder<StorableTestBasic> builder
            = new SyntheticStorableReferenceBuilder<StorableTestBasic>(test.mClass, false);

            for (IndexDef refProp : test.mProps) {
                StorableProperty storableProperty = ((StorableProperty) info.getAllProperties()
                                                                            .get(refProp.mProp));
                builder.addKeyProperty(refProp.mProp, refProp.getDir());
            }
            builder.build();
            Class s = builder.getStorableClass();
            validateIndexEntry(test, s);
            exerciseStorable(s);

            StorableTestBasic master =
                mRepository.storageFor(StorableTestBasic.class).prepare();
            populate(master);
            master.insert();

            Storable index = mRepository.storageFor(s).prepare();
            builder.setAllProperties(index, master);
            index.insert();

            Storable indexChecker = mRepository.storageFor(s).prepare();
            builder.setAllProperties(indexChecker, master);
            assertTrue(indexChecker.tryLoad());

            StorableTestBasic masterChecker = builder.loadMaster(indexChecker);
            assertEquals(master, masterChecker);

            assertTrue(builder.isConsistent(index, master));
            masterChecker =
                mRepository.storageFor(StorableTestBasic.class).prepare();
            master.copyAllProperties(masterChecker);
            assertTrue(builder.isConsistent(index, masterChecker));
            masterChecker.setId(-42);
            assertFalse(builder.isConsistent(index, masterChecker));

        }
    }

    /**
     * @param <S>
     * @param storableType
     * @throws SupportException
     * @throws RepositoryException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws PersistException
     * @throws FetchException
     */
    protected <T extends Storable> void exerciseStorable(Class<T> storableType) throws SupportException, RepositoryException, IllegalAccessException, InvocationTargetException, PersistException, FetchException {
        T persister = mRepository.storageFor(storableType).prepare();
        Map<String, Object> valueMap = populate(persister);

        persister.insert();
        T reader = mRepository.storageFor(storableType).prepare();
        persister.copyPrimaryKeyProperties(reader);
        assertTrue(reader.tryLoad());

        for (Method method : storableType.getMethods()) {
            if (method.getName().startsWith("get") ) {
                Class returnType = method.getReturnType();
                Class[] paramTypes = method.getParameterTypes();
                if (VENDOR.hasValue(returnType) && paramTypes.length == 0) {
                    Object expectedValue = valueMap.get(method.getName().substring(3));
                    Object actualValue = method.invoke(persister, (Object[]) null);
                    assertEquals(expectedValue, actualValue);
                }
            }
        }
    }

    /**
     * Populates a storable with random values.  Any complex properties will be skipped.
     * @param storable to populate
     * @return map from property name to value set into it
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    protected <T extends Storable> Map<String, Object> populate(T storable)
            throws IllegalAccessException, InvocationTargetException {
        ValueVendor vendor = new ValueVendor();
        Map<String, Object> valueMap = new HashMap<String, Object>();
        vendor.reset();
        for (Method method : storable.getClass().getMethods()) {
            if (method.getName().startsWith("set") ) {
                Class[] paramTypes = method.getParameterTypes();
                Class type = paramTypes[0];
                if (vendor.hasValue(type)) {
                    Object value = vendor.getValue(type);
                    method.invoke(storable, new Object[] {value});
                    valueMap.put(method.getName().substring(3), value);
                }
            }
        }
        return valueMap;
    }




    protected void validateIndexEntry(TestDef test, Class syntheticClass) {
        assertEquals(test.mClassName, syntheticClass.getName());
        Map<String, String[]> expectedMethods = new HashMap<String, String[]>();
        if (null != test.mMethods) {
            for (MethodDef m : test.mMethods) {
                expectedMethods.put(m.mName, m.mAnnotations);
            }
        }

        Class iface = syntheticClass.getInterfaces()[0];
        assertEquals(test.mTestMoniker, Storable.class, iface);

        boolean missed = false;
        int storableIfaceMethodCount = 0;
        int methodCount = 0;
        for (Method m : syntheticClass.getMethods()) {
            if (s_storableInterfaceMethods.contains(m.getName())) {
                storableIfaceMethodCount++;
            } else {
                Set expectedAnnotations = null;
                if (expectedMethods.containsKey(m.getName())) {
                    expectedAnnotations = new HashSet();
                    String[] expectedAnnotationArray = expectedMethods.get(m.getName());
                    if (null != expectedAnnotationArray) {
                        for (String a : expectedAnnotationArray) {
                            expectedAnnotations.add(a);
                        }
                    }
                } else {
                    System.out.println("missed method " + methodCount
                            + "\nMethodDef(\"" + m.getName() + "\", null),");
                    missed = true;
                }

                if (expectedAnnotations != null) {
                    assertEquals(test.mTestMoniker + ": " + m.getName(),
                                 expectedAnnotations.size(),
                                 m.getDeclaredAnnotations().length);
                } else {
                    assertEquals(test.mTestMoniker + ": " + m.getName(),
                                 0,
                                 m.getDeclaredAnnotations().length);
                }

                for (Annotation a : m.getDeclaredAnnotations()) {
                    if (missed) {
                        System.out.println("    " + a);
                    }
                    if (expectedAnnotations != null) {
                        if (!expectedAnnotations.contains(a.toString())) {
                            boolean found = false;
                            for (Object candidate : expectedAnnotations.toArray()) {
                                if (a.toString().startsWith((String) candidate)) {
                                    found = true;
                                    break;
                                }
                                // Special case to handle arbitrary ordering of
                                // annotation parameters.
                                if (candidate == TEXT && a.toString().startsWith(TEXT_2)) {
                                    found = true;
                                    break;
                                }
                            }
                            assertTrue(test.mTestMoniker + ':' + m.getName()
                                    + " -- unexpected annotation " + a, found);
                        } // else we're ok
                    }
                    ;
                }
            }
            methodCount++;
        }
        assertEquals(test.mTestMoniker,
                     s_storableInterfaceMethods.size(),
                     storableIfaceMethodCount);
        assertFalse(test.mTestMoniker, missed);
    }

    static class MethodDef {
        String mName;

        String[] mAnnotations;

        public MethodDef(String name, String[] annotations) {
            mName = name;
            mAnnotations = annotations;
        }

        /*
         * (non-Javadoc)
         *
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString()
        {
            String annotations = null;
            if (null != mAnnotations) {
                for (int i = 0; i < mAnnotations.length; i++) {
                    if (null == annotations) {
                        annotations = "";
                    } else {
                        annotations += ", ";
                    }
                    annotations += mAnnotations[i];
                }
            } else {
                annotations = " -- ";
            }
            return mName + " [" + annotations + ']';
        }

    }

    /**
     * More intuitive mechanism for defining index properties; propClass is
     * optionally used for synthetic references (since we don't formally define
     * properties in this test environment)
     */
    public static class IndexDef {
        private String mProp;

        private Direction mDir;

        public IndexDef(String prop, Direction dir) {
            mProp = prop;
            mDir = dir;
        }

        public String getProp() {
            return mProp;
        }

        public Direction getDir() {
            return mDir;
        }
    }

    public static class TestDef {
        public String mTestMoniker;

        public Class mClass;

        public String mClassName;

        public IndexDef[] mProps;

        public MethodDef[] mMethods;

        public SyntheticProperty[] mSynths;

        public TestDef(String moniker,
                       Class aClass,
                       String aClassName,
                       IndexDef[] props,
                       MethodDef[] methods,
                       SyntheticProperty[] synths) {
            mTestMoniker = moniker;
            mClass = aClass;
            mClassName = aClassName;
            mProps = props;
            mMethods = methods;
            mSynths = synths;
        }

        public String toString() {
            return mTestMoniker;
        }
    }

    public interface ValueGetter {
        public void reset();
        public Object getValue();
    }

    static class BoolGetter implements ValueGetter {
        final boolean start = false;
        boolean mNext = start;
        public void reset() {
            mNext = start;
        }
        public Boolean getValue() {
            mNext = !mNext;
            return mNext;
        }
    }

    static class IntGetter implements ValueGetter {
        // hang onto the seed so we get a reproducible stream
        int mSeed;
        Random mNext;

        IntGetter() {
            reset();
        }

        public  void reset() {
            Random seeder = new Random();
            while (mSeed == 0) {
                mSeed = seeder.nextInt();
            }
            mNext = new Random(mSeed);
        }

        public Integer getValue() {
            return mNext.nextInt();
        }

        public Integer getPositiveValue() {
            return Math.abs(mNext.nextInt());
        }
    }

    static class LongGetter implements ValueGetter {
        final long start = 56789;
        long mNext = start;
        public void reset() {
            mNext = start;
        }
        public Long getValue() {
            return mNext++;
        }
    }

    static class FloatGetter implements ValueGetter {
        final float start = (float) Math.PI;
        float mNext = start;
        public void reset() {
            mNext = start;
        }
        public Float getValue() {
            float next = mNext;
            mNext = next + 0.1f;
            return next;
        }
    }

    static class DoubleGetter implements ValueGetter {
        final double start = Math.PI;
        double mNext = start;
        public void reset() {
            mNext = start;
        }
        public Double getValue() {
            double next = mNext;
            mNext = next + 0.1;
            return next;
        }
    }

    static class CharGetter implements ValueGetter {

        static final String source = "zookeepers fly piglatin homogeneous travesty";

        IntGetter mNext = new IntGetter();

        public  void reset() {
            mNext.reset();
        }


        public Character getValue() {
            return source.charAt(mNext.getPositiveValue() % source.length());
        }
    }

    static class StringGetter implements ValueGetter {
        IntGetter mSizer = new IntGetter();
        CharGetter mNext = new CharGetter();

        public  void reset() {
            mSizer.reset();
            mNext.reset();
        }
        public String getValue() {
            int size = mSizer.getPositiveValue() % 255;
            StringBuilder sb = new StringBuilder(size);
            for (int i = 0; i<size; i++) {
                sb.append(mNext.getValue());
            }
            return sb.toString();
        }
    }

    static class DateTimeGetter implements ValueGetter {
        static final DateTime start = new DateTime("2005-01-02");
        DateTime mNext = start;
        public void reset() {
            mNext = start;
        }
        public DateTime getValue() {
            DateTime next = mNext;
            mNext = mNext.dayOfYear().addToCopy(1);
            return next;
        }
    }

    static class ValueVendor {
        Map<Class, ValueGetter> getters = new HashMap(10);
        ValueVendor() {
            getters.put(String.class, new StringGetter());
            getters.put(DateTime.class, new DateTimeGetter());

            getters.put(Boolean.class, new BoolGetter());
            getters.put(Integer.class, new IntGetter());
            getters.put(Long.class, new LongGetter());
            getters.put(Float.class, new FloatGetter());
            getters.put(Double.class, new DoubleGetter());
            getters.put(Character.class, new CharGetter());

            getters.put(boolean.class, new BoolGetter());
            getters.put(int.class, new IntGetter());
            getters.put(long.class, new LongGetter());
            getters.put(float.class, new FloatGetter());
            getters.put(double.class, new DoubleGetter());
            getters.put(char.class, new CharGetter());
        }

        void reset() {
            for (Iterator iter = getters.values().iterator(); iter.hasNext();) {
                ValueGetter getter = (ValueGetter) iter.next();
                getter.reset();
            }
        }

        boolean hasValue(Class type) {
            return getters.containsKey(type);
        }


        Object getValue(Class type) {
            return getters.get(type).getValue();
        }
    }
}
