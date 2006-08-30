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

/**
 * General purpose class for throttling work relative to its actual measured
 * performance. To throttle a task, call the throttle method each time a unit
 * of work has been performed. It computes a rolling average for the amount of
 * time it takes to perform some work, and then it sleeps a calculated amount
 * of time to throttle back.
 *
 * <p>Instances are intended for use by one thread, and so they are not
 * thread-safe.
 *
 * @author Brian S O'Neill
 */
public class Throttle {
    private final double[] mWorkTimeSamples;

    // Next index in mSamples to use.
    private int mSampleIndex;

    private double mWorkTimeSum;

    // Amount of samples gathered.
    private int mSampleCount;

    private long mLastTimestampNanos;

    private double mSleepRequiredNanos;

    /**
     * @param windowSize amount of samples to keep in the rolling average
     */
    public Throttle(int windowSize) {
        if (windowSize < 1) {
            throw new IllegalArgumentException();
        }

        mWorkTimeSamples = new double[windowSize];
    }

    /**
     * @param desiredSpeed 1.0 = perform work at full speed,
     * 0.5 = perform work at half speed, 0.0 = fully suspend work
     * @param sleepPrecisionMillis sleep precision, in milliseconds. Typical
     * value is 10 to 100 milliseconds.
     */
    public void throttle(double desiredSpeed, long sleepPrecisionMillis)
        throws InterruptedException
    {
        long timestampNanos = System.nanoTime();

        int sampleCount = mSampleCount;

        int index = mSampleIndex;
        double workTime = timestampNanos - mLastTimestampNanos;
        double workTimeSum = mWorkTimeSum + workTime;

        double[] workTimeSamples = mWorkTimeSamples;

        if (sampleCount >= workTimeSamples.length) {
            workTimeSum -= workTimeSamples[index];
            double average = workTimeSum / sampleCount;

            double sleepTimeNanos = (average / desiredSpeed) - average;
            double sleepRequiredNanos = mSleepRequiredNanos + sleepTimeNanos;

            if (sleepRequiredNanos > 0.0) {
                double sleepRequiredMillis = sleepRequiredNanos * (1.0 / 1000000);

                long millis;
                if (sleepRequiredMillis > Long.MAX_VALUE) {
                    millis = Long.MAX_VALUE;
                } else {
                    millis = Math.max(sleepPrecisionMillis, (long) sleepRequiredMillis);
                }

                Thread.sleep(millis);

                long nextNanos = System.nanoTime();

                // Subtract off time spent in this method, including sleep.
                sleepRequiredNanos -= (nextNanos - timestampNanos);
                timestampNanos = nextNanos;
            }

            mSleepRequiredNanos = sleepRequiredNanos;
        }

        workTimeSamples[index] = workTime;
        index++;
        if (index >= workTimeSamples.length) {
            index = 0;
        }
        mSampleIndex = index;

        mWorkTimeSum = workTimeSum;

        if (sampleCount < workTimeSamples.length) {
            mSampleCount = sampleCount + 1;
        }

        mLastTimestampNanos = timestampNanos;
    }

    /**
     * Test program which exercises the CPU in an infinite loop, throttled by
     * the amount given in args[0]. On a machine performing no other work, the
     * average CPU load should be about the same as the throttled speed.
     *
     * @param args args[0] - desired speed, 0.0 to 1.0
     */
    public static void main(String[] args) throws Exception {
        Throttle t = new Throttle(50);
        double desiredSpeed = Double.parseDouble(args[0]);
        while (true) {
            new java.util.Date().toString();
            t.throttle(desiredSpeed, 100);
        }
    }
}
