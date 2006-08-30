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

package com.amazon.carbonado.repo.logging;

/**
 * Log implementation that uses Jakarta Commons Logging at debug level.
 *
 * @author Brian S O'Neill
 */
public class CommonsLog implements Log {
    private final org.apache.commons.logging.Log mLog;

    public CommonsLog(org.apache.commons.logging.Log log) {
        mLog = log;
    }

    public CommonsLog(Class clazz) {
        mLog = org.apache.commons.logging.LogFactory.getLog(clazz);
    }

    public boolean isEnabled() {
        return mLog.isDebugEnabled();
    }

    public void write(String message) {
        mLog.debug(message);
    }
}
