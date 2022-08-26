/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.csp.sentinel.slots.statistic.base;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.csp.sentinel.util.AssertUtil;
import com.alibaba.csp.sentinel.util.TimeUtil;

/**
 * <p>
 * Basic data structure for statistic metrics in Sentinel.
 * </p>
 * <p>
 * Leap array use sliding window algorithm to count data. Each bucket cover {@code windowLengthInMs} time span,
 * and the total time span is {@link #intervalInMs}, so the total bucket amount is:
 * {@code sampleCount = intervalInMs / windowLengthInMs}.
 * </p>
 *
 * <p>
 * LeapArray是一个环形数组，因为时间是无限的，数组长度不可能无限，因此数组中每一个格子放入一个时间窗（window），当数组放满后，角标归0，覆盖最初的window。
 *
 * 添加数据的时候，先判断当前走到哪个窗口了（当前时间(s) % 60 即可），然后需要判断这个窗口是否是过期数据，如果是过期数据（窗口代表的时间距离当前已经超过 1 分钟），需要先重置这个窗口实例的数据。
 * 统计数据同理，如统计过去一分钟的 QPS 数据，就是将每个窗口的值相加，当中需要判断窗口数据是否是过期数据，即判断窗口的 WindowWrap 实例是否是一分钟内的数据。
 *
 * 时间窗口：大的窗口
 * 样本窗口：在一个时间窗口内，分出来的多个小窗口，每一个小窗口就是一个样本窗口。
 * </p>
 *
 * @param <T> type of statistic data
 * @author jialiang.linjl
 * @author Eric Zhao
 * @author Carpenter Lee
 */
public abstract class LeapArray<T> {
    /**
     * 每一个样本窗口的长度  =  intervalInMs / sampleCount
     * 对于秒级，其值是500
     * 对于分钟级，其值是1000
     */
    protected int windowLengthInMs;
    /**
     * 一个时间窗口里面样本窗口的数量
     * 对于秒级，其值是2
     * 对于分钟级，其值是60
     */
    protected int sampleCount;
    /**
     * 一个时间窗口的长度，单位为毫秒
     * 对于秒级，其值是1000
     * 对于分钟级，其值是60000
     */
    protected int intervalInMs;
    /**
     * 一个时间窗口的长度，单位为秒，= intervalInMs / 1000
     * 对于秒级，其值是：1
     * 对于分钟级，其值是：60
     */
    private double intervalInSecond;

    /**
     *  一个时间窗中每一个样本窗口，是一个WindowWrap对象数组
     *  由构造函数中的this.array = new AtomicReferenceArray<>(sampleCount)
     *  可知：array最大只有sampleCount个元素，这是一个环形数组
     */
    protected final AtomicReferenceArray<WindowWrap<T>> array;

    /**
     * The conditional (predicate) update lock is used only when current bucket is deprecated.
     */
    private final ReentrantLock updateLock = new ReentrantLock();

    /**
     * The total bucket count is: {@code sampleCount = intervalInMs / windowLengthInMs}.
     *
     * @param sampleCount  bucket count of the sliding window
     * @param intervalInMs the total time interval of this {@link LeapArray} in milliseconds
     */
    public LeapArray(int sampleCount, int intervalInMs) {
        AssertUtil.isTrue(sampleCount > 0, "bucket count is invalid: " + sampleCount);
        AssertUtil.isTrue(intervalInMs > 0, "total time interval of the sliding window should be positive");
        AssertUtil.isTrue(intervalInMs % sampleCount == 0, "time span needs to be evenly divided");
        // 单个窗口桶的时间长度(以毫秒为单位) = 总时间长度 / 样本窗口数量。在秒级统计中，是1000/2=500
        this.windowLengthInMs = intervalInMs / sampleCount;
        // 总时间跨度，以毫秒为单位
        this.intervalInMs = intervalInMs;
        // 总时间跨度，以秒为单位
        this.intervalInSecond = intervalInMs / 1000.0;
        // 数组长度，即样本窗口的个数
        this.sampleCount = sampleCount;
        // 数组元素为WindowWrap，WindowWrap保存了MetricBucket，在它内部才保存真正的指标数据
        this.array = new AtomicReferenceArray<>(sampleCount);
    }

    /**
     * Get the bucket at current timestamp.
     *
     * @return the bucket at current timestamp
     */
    public WindowWrap<T> currentWindow() {
        return currentWindow(TimeUtil.currentTimeMillis());
    }

    /**
     * Create a new statistic value for bucket.
     *
     * @param timeMillis current time in milliseconds
     * @return the new empty bucket
     */
    public abstract T newEmptyBucket(long timeMillis);

    /**
     * Reset given bucket to provided start time and reset the value.
     *
     * @param startTime  the start time of the bucket in milliseconds
     * @param windowWrap current bucket
     * @return new clean bucket at given start time
     */
    protected abstract WindowWrap<T> resetWindowTo(WindowWrap<T> windowWrap, long startTime);

    /**
     * 获取窗口位置的算法：（当前时间/样本窗口长度）% 样本窗口个数
     * 注意：这里返回的是每一个区间窗口内样本窗口的位置
     *
     * 先将当前时间按照统计时长分段，得到当前时间对应的分段ID。
     * 因为窗口数组是固定的，所以随着时间线向前发展，会不断的顺序循环使用数组中的窗口。
     * 所以使用当前时间对应的分段ID与窗口数组的长度求余得到当前时间对应的窗口在窗口数组中的下标，
     * 拿到这个下标后，接着就是在循环中获取这个下标对应的窗口了。
     * @param timeMillis 当前时间
     * @return
     */
    private int calculateTimeIdx(/*@Valid*/ long timeMillis) {
        // 当前时间所属的分段ID = 当前时间 / 每个小窗口的长度
        // 比如windowLengthInMs=500，则1-499的分段ID都为0，500-999的分段ID为1,1000-1499的分段ID为2,1500-1999的分段ID为3，
        // 虽然这个分段ID可能会很大，但经过下一步的取模后，就只可能落在某个窗口了。
        long timeId = timeMillis / windowLengthInMs;
        // Calculate current index so we can map the timestamp to the leap array.
        // 当前时间所属的窗口序号 = 分段ID % 样本窗口数量，其最终的值只可能在0-样本窗口数量之间
        return (int)(timeId % array.length());
    }


    /**
     * 计算当前窗口的开始时间：当前时间-当前时间 % 窗口长度
     * @param timeMillis
     * @return
     */
    protected long calculateWindowStart(/*@Valid*/ long timeMillis) {
        return timeMillis - timeMillis % windowLengthInMs;
    }

    /**
     * 1、先计算当前时间在窗口中的位置，得到当前窗口
     * 2、再计算当前时间所在的窗口的开始时间
     * 3、比较当前时间和窗口的开始时间，进行处理：要么新建窗口并返回当前窗口，要么直接返回当前窗口，要么重置窗口后返回当前窗口
     *
     * Get bucket item at provided timestamp.
     *
     * @param timeMillis a valid timestamp in milliseconds
     * @return current bucket item at provided timestamp if the time is valid; null if time is invalid
     */
    public WindowWrap<T> currentWindow(long timeMillis) {
        //当前时间如果小于0，返回空
        if (timeMillis < 0) {
            return null;
        }

        // 计算时间窗口的索引，实际上，因为窗口是一个环形数组，所以，其范围只可能在0 ~（sampleCount-1）
        // 取到idx后，就可以得到当前时间所在窗口的开始时间，通过对比这个窗口的开始时间和下一步计算出来当前时间所在窗口的开始时间，就能定位到窗口的位置了。
        int idx = calculateTimeIdx(timeMillis);
        // Calculate current bucket start time.
        // 计算当前时间所在的窗口的开始时间，这个可以理解为时间是一直往前走的，每一个当前时间，必然属于某一个窗口，此处就是返回这个窗口的开始时间
        long windowStart = calculateWindowStart(timeMillis);

        /*
         * Get bucket item at given time from the array.
         *
         * (1) Bucket is absent, then just create a new bucket and CAS update to circular array.
         * (2) Bucket is up-to-date, then just return the bucket.
         * (3) Bucket is deprecated, then reset current bucket and clean all deprecated buckets.
         *
         */
        /*
         * 根据下脚标在环形数组中获取滑动窗口（桶）
         *
         * (1) 如果桶不存在则创建新的桶，并通过CAS将新桶赋值到数组下标位。否则走以下逻辑
         * (2) 如果桶的开始时间等于刚刚算出来的时间，那么返回当前获取到的桶。
         * (3) 如果桶的开始时间小于刚刚算出来的开始时间，那么说明这个桶是上一圈用过的桶，重置当前桶。注意：数组的大小是固定的，每次都重复下标，相当于一个环形
         * (4) 如果桶的开始时间大于刚刚算出来的开始时间，理论上不应该出现这种情况。
         */
        while (true) {
            //在窗口数组中获得窗口，如果样本窗口数为2，则这个array最多只有2个元素，如果样本窗口数为60，则这个array最多只有60个元素
            WindowWrap<T> old = array.get(idx);
            if (old == null) {
                /*
                 *     B0       B1      B2    NULL      B4
                 * ||_______|_______|_______|_______|_______||___
                 * 200     400     600     800     1000    1200  timestamp
                 *                             ^
                 *                          time=888
                 *            bucket is empty, so create new and update
                 *
                 * If the old bucket is absent, then we create a new bucket at {@code windowStart},
                 * then try to update circular array via a CAS operation. Only one thread can
                 * succeed to update, while other threads yield its time slice.
                 *
                 * 比如当前时间是888，根据计算得到的数组窗口位置是个空，所以直接创建一个新窗口就好了
                 */
                WindowWrap<T> window = new WindowWrap<T>(windowLengthInMs, windowStart, newEmptyBucket(timeMillis));
                // 基于CAS写入数组，避免线程安全问题
                if (array.compareAndSet(idx, null, window)) {
                    // Successfully updated, return the created bucket.
                    // 写入成功，返回新的 window
                    return window;
                } else {
                    // Contention failed, the thread will yield its time slice to wait for bucket available.
                    // 写入失败，说明有并发更新，等待其它人更新完成即可
                    Thread.yield();
                }
            } else if (windowStart == old.windowStart()) {
                /*
                 *     B0       B1      B2     B3      B4
                 * ||_______|_______|_______|_______|_______||___
                 * 200     400     600     800     1000    1200  timestamp
                 *                             ^
                 *                          time=888
                 *            startTime of Bucket 3: 800, so it's up-to-date
                 *
                 * If current {@code windowStart} is equal to the start timestamp of old bucket,
                 * that means the time is within the bucket, so directly return the bucket.
                 *
                 * 这个更好了，刚好等于，直接返回就行
                 */
                return old;
            } else if (windowStart > old.windowStart()) {
                /*
                 *   (old)
                 *             B0       B1      B2    NULL      B4
                 * |_______||_______|_______|_______|_______|_______||___
                 * ...    1200     1400    1600    1800    2000    2200  timestamp
                 *                              ^
                 *                           time=1676
                 *          startTime of Bucket 2: 400, deprecated, should be reset
                 *
                 * If the start timestamp of old bucket is behind provided time, that means
                 * the bucket is deprecated. We have to reset the bucket to current {@code windowStart}.
                 * Note that the reset and clean-up operations are hard to be atomic,
                 * so we need a update lock to guarantee the correctness of bucket update.
                 *
                 * The update lock is conditional (tiny scope) and will take effect only when
                 * bucket is deprecated, so in most cases it won't lead to performance loss.
                 *
                 *  这个要当成圆形理解就好了，之前如果是1200一个完整的圆形，然后继续从1200开始，如果现在时间是1676，落在在B2的位置，
                 *  窗口开始时间是1600，获取到的old时间其实会是600，所以肯定是过期了，直接重置窗口就可以了
                 */
                if (updateLock.tryLock()) {
                    try {
                        // Successfully get the update lock, now we reset the bucket.
                        // 获取并发锁，覆盖旧窗口并返回
                        return resetWindowTo(old, windowStart);
                    } finally {
                        updateLock.unlock();
                    }
                } else {
                    // Contention failed, the thread will yield its time slice to wait for bucket available.
                    // 获取锁失败，等待其它线程处理就可以了
                    Thread.yield();
                }
            } else if (windowStart < old.windowStart()) {
                // 这个不太可能出现。除非是时钟回拨
                // Should not go through here, as the provided time is already behind.
                return new WindowWrap<T>(windowLengthInMs, windowStart, newEmptyBucket(timeMillis));
            }
        }
    }

    /**
     * Get the previous bucket item before provided timestamp.
     *
     * @param timeMillis a valid timestamp in milliseconds
     * @return the previous bucket item before provided timestamp
     */
    public WindowWrap<T> getPreviousWindow(long timeMillis) {
        if (timeMillis < 0) {
            return null;
        }
        int idx = calculateTimeIdx(timeMillis - windowLengthInMs);
        timeMillis = timeMillis - windowLengthInMs;
        WindowWrap<T> wrap = array.get(idx);

        if (wrap == null || isWindowDeprecated(wrap)) {
            return null;
        }

        if (wrap.windowStart() + windowLengthInMs < (timeMillis)) {
            return null;
        }

        return wrap;
    }

    /**
     * Get the previous bucket item for current timestamp.
     *
     * @return the previous bucket item for current timestamp
     */
    public WindowWrap<T> getPreviousWindow() {
        return getPreviousWindow(TimeUtil.currentTimeMillis());
    }

    /**
     * Get statistic value from bucket for provided timestamp.
     *
     * @param timeMillis a valid timestamp in milliseconds
     * @return the statistic value if bucket for provided timestamp is up-to-date; otherwise null
     */
    public T getWindowValue(long timeMillis) {
        if (timeMillis < 0) {
            return null;
        }
        int idx = calculateTimeIdx(timeMillis);

        WindowWrap<T> bucket = array.get(idx);

        if (bucket == null || !bucket.isTimeInWindow(timeMillis)) {
            return null;
        }

        return bucket.value();
    }

    /**
     * Check if a bucket is deprecated, which means that the bucket
     * has been behind for at least an entire window time span.
     *
     * @param windowWrap a non-null bucket
     * @return true if the bucket is deprecated; otherwise false
     */
    public boolean isWindowDeprecated(/*@NonNull*/ WindowWrap<T> windowWrap) {
        return isWindowDeprecated(TimeUtil.currentTimeMillis(), windowWrap);
    }

    public boolean isWindowDeprecated(long time, WindowWrap<T> windowWrap) {
        // 当前时间 - 窗口开始时间  是否大于 滑动窗口的最大间隔（1秒）
        // 也就是说，我们要统计的时 距离当前时间1秒内的 小窗口的 count之和
        return time - windowWrap.windowStart() > intervalInMs;
    }

    /**
     * Get valid bucket list for entire sliding window.
     * The list will only contain "valid" buckets.
     *
     * @return valid bucket list for entire sliding window.
     */
    public List<WindowWrap<T>> list() {
        return list(TimeUtil.currentTimeMillis());
    }

    public List<WindowWrap<T>> list(long validTime) {
        int size = array.length();
        List<WindowWrap<T>> result = new ArrayList<WindowWrap<T>>(size);

        for (int i = 0; i < size; i++) {
            WindowWrap<T> windowWrap = array.get(i);
            if (windowWrap == null || isWindowDeprecated(validTime, windowWrap)) {
                continue;
            }
            result.add(windowWrap);
        }

        return result;
    }

    /**
     * Get all buckets for entire sliding window including deprecated buckets.
     *
     * @return all buckets for entire sliding window
     */
    public List<WindowWrap<T>> listAll() {
        int size = array.length();
        List<WindowWrap<T>> result = new ArrayList<WindowWrap<T>>(size);

        for (int i = 0; i < size; i++) {
            WindowWrap<T> windowWrap = array.get(i);
            if (windowWrap == null) {
                continue;
            }
            result.add(windowWrap);
        }

        return result;
    }

    /**
     * Get aggregated value list for entire sliding window.
     * The list will only contain value from "valid" buckets.
     *
     * @return aggregated value list for entire sliding window
     */
    public List<T> values() {
        return values(TimeUtil.currentTimeMillis());
    }

    public List<T> values(long timeMillis) {
        if (timeMillis < 0) {
            return new ArrayList<T>();
        }
        // 创建空集合，大小等于 LeapArray长度
        int size = array.length();
        List<T> result = new ArrayList<T>(size);

        // 遍历 LeapArray
        for (int i = 0; i < size; i++) {
            // 获取每一个小窗口
            WindowWrap<T> windowWrap = array.get(i);
            // 判断这个小窗口是否在 滑动窗口时间范围内（1秒内）
            if (windowWrap == null || isWindowDeprecated(timeMillis, windowWrap)) {
                // 不在范围内，则跳过
                continue;
            }
            // 在范围内，则添加到集合中
            result.add(windowWrap.value());
        }
        // 返回集合
        return result;
    }

    /**
     * Get the valid "head" bucket of the sliding window for provided timestamp.
     * Package-private for test.
     *
     * @param timeMillis a valid timestamp in milliseconds
     * @return the "head" bucket if it exists and is valid; otherwise null
     */
    WindowWrap<T> getValidHead(long timeMillis) {
        // Calculate index for expected head time.
        int idx = calculateTimeIdx(timeMillis + windowLengthInMs);

        WindowWrap<T> wrap = array.get(idx);
        if (wrap == null || isWindowDeprecated(wrap)) {
            return null;
        }

        return wrap;
    }

    /**
     * Get the valid "head" bucket of the sliding window at current timestamp.
     *
     * @return the "head" bucket if it exists and is valid; otherwise null
     */
    public WindowWrap<T> getValidHead() {
        return getValidHead(TimeUtil.currentTimeMillis());
    }

    /**
     * Get sample count (total amount of buckets).
     *
     * @return sample count
     */
    public int getSampleCount() {
        return sampleCount;
    }

    /**
     * Get total interval length of the sliding window in milliseconds.
     *
     * @return interval in second
     */
    public int getIntervalInMs() {
        return intervalInMs;
    }

    /**
     * Get total interval length of the sliding window.
     *
     * @return interval in second
     */
    public double getIntervalInSecond() {
        return intervalInSecond;
    }

    public void debug(long time) {
        StringBuilder sb = new StringBuilder();
        List<WindowWrap<T>> lists = list(time);
        sb.append("Thread_").append(Thread.currentThread().getId()).append("_");
        for (WindowWrap<T> window : lists) {
            sb.append(window.windowStart()).append(":").append(window.value().toString());
        }
        System.out.println(sb.toString());
    }

    public long currentWaiting() {
        // TODO: default method. Should remove this later.
        return 0;
    }

    public void addWaiting(long time, int acquireCount) {
        // Do nothing by default.
        throw new UnsupportedOperationException();
    }
}
