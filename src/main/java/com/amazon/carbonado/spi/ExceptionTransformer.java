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

import java.io.InterruptedIOException;
import java.nio.channels.ClosedByInterruptException;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.FetchInterruptedException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.PersistInterruptedException;
import com.amazon.carbonado.RepositoryException;

/**
 * Supports transforming arbitrary exceptions into appropriate repository
 * exceptions. Repositories will likely extend this class, providing custom
 * transformation rules.
 *
 * @author Brian S O'Neill
 */
public class ExceptionTransformer {
    private static ExceptionTransformer cInstance;

    /**
     * Returns a generic instance.
     */
    public static ExceptionTransformer getInstance() {
        if (cInstance == null) {
            cInstance = new ExceptionTransformer();
        }
        return cInstance;
    }

    public ExceptionTransformer() {
    }

    /**
     * Transforms the given throwable into an appropriate fetch exception. If
     * it already is a fetch exception, it is simply casted.
     *
     * @param e required exception to transform
     * @return FetchException, never null
     */
    public FetchException toFetchException(Throwable e) {
        FetchException fe = transformIntoFetchException(e);
        if (fe != null) {
            return fe;
        }

        Throwable cause = e.getCause();
        if (cause != null) {
            fe = transformIntoFetchException(cause);
            if (fe != null) {
                return fe;
            }
        } else {
            cause = e;
        }

        return new FetchException(cause);
    }

    /**
     * Transforms the given throwable into an appropriate persist exception. If
     * it already is a persist exception, it is simply casted.
     *
     * @param e required exception to transform
     * @return PersistException, never null
     */
    public PersistException toPersistException(Throwable e) {
        PersistException pe = transformIntoPersistException(e);
        if (pe != null) {
            return pe;
        }

        Throwable cause = e.getCause();
        if (cause != null) {
            pe = transformIntoPersistException(cause);
            if (pe != null) {
                return pe;
            }
        } else {
            cause = e;
        }

        return new PersistException(cause);
    }

    /**
     * Transforms the given throwable into an appropriate repository
     * exception. If it already is a repository exception, it is simply casted.
     *
     * @param e required exception to transform
     * @return RepositoryException, never null
     */
    public RepositoryException toRepositoryException(Throwable e) {
        RepositoryException re = transformIntoRepositoryException(e);
        if (re != null) {
            return re;
        }

        Throwable cause = e.getCause();
        if (cause != null) {
            re = transformIntoRepositoryException(cause);
            if (re != null) {
                return re;
            }
        } else {
            cause = e;
        }

        return new RepositoryException(cause);
    }

    /**
     * Override to support custom transformations, returning null if none is
     * applicable. Be sure to call super first. If it returns non-null, return
     * that result.
     *
     * @param e required exception to transform
     * @return FetchException, or null if no applicable transform
     */
    protected FetchException transformIntoFetchException(Throwable e) {
        if (e instanceof FetchException) {
            return (FetchException) e;
        }
        if (e instanceof InterruptedIOException ||
            e instanceof ClosedByInterruptException) {
            return new FetchInterruptedException(e);
        }
        return null;
    }

    /**
     * Override to support custom transformations, returning null if none is
     * applicable. Be sure to call super first. If it returns non-null, return
     * that result.
     *
     * @param e required exception to transform
     * @return PersistException, or null if no applicable transform
     */
    protected PersistException transformIntoPersistException(Throwable e) {
        if (e instanceof PersistException) {
            return (PersistException) e;
        }
        if (e instanceof InterruptedIOException) {
            return new PersistInterruptedException(e);
        }
        if (e instanceof FetchException) {
            return ((FetchException) e).toPersistException();
        }
        return null;
    }

    /**
     * Override to support custom transformations, returning null if none is
     * applicable. Be sure to call super first. If it returns non-null, return
     * that result.
     *
     * @param e required exception to transform
     * @return RepositoryException, or null if no applicable transform
     */
    protected RepositoryException transformIntoRepositoryException(Throwable e) {
        if (e instanceof RepositoryException) {
            return (RepositoryException) e;
        }
        RepositoryException re = transformIntoFetchException(e);
        if (re != null) {
            return re;
        }
        re = transformIntoPersistException(e);
        if (re != null) {
            return re;
        }
        return null;
    }
}
