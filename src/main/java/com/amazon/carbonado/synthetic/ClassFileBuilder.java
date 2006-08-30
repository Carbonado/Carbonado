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

import org.cojen.classfile.ClassFile;
import org.cojen.util.ClassInjector;

/**
 * Simple interface representing a partially filled class and injector pair.
 *
 * <P>This is abstract because it provides no mechanism for defining the
 * classfile or injector; subclasses must provide them.
 *
 * @author Don Schneider
 */
public abstract class ClassFileBuilder {

    /**
     * Partially hydrogenated class operators
     */
    protected ClassFile mClassFile;
    protected ClassInjector mInjector;

    /**
     * @return Returns the classFile.
     */
    public ClassFile getClassFile() {
        return mClassFile;
    }

    /**
     * @return Returns the injector.
     */
    public ClassInjector getInjector() {
        return mInjector;
    }

    /**
     * Defines the class for this generator
     */
    @SuppressWarnings("unchecked")
    public Class build() {
        return getInjector().defineClass(getClassFile());
    }
}
