/*
 * Copyright 1999-2019 Alibaba Group Holding Ltd.
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
package com.alibaba.csp.sentinel.slots.block.degrade.circuitbreaker;

import java.util.List;
import java.util.concurrent.atomic.LongAdder;

import com.alibaba.csp.sentinel.Entry;
import com.alibaba.csp.sentinel.context.Context;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRule;
import com.alibaba.csp.sentinel.slots.statistic.base.LeapArray;
import com.alibaba.csp.sentinel.slots.statistic.base.WindowWrap;
import com.alibaba.csp.sentinel.util.AssertUtil;

import static com.alibaba.csp.sentinel.slots.block.RuleConstant.DEGRADE_GRADE_EXCEPTION_COUNT;
import static com.alibaba.csp.sentinel.slots.block.RuleConstant.DEGRADE_GRADE_EXCEPTION_RATIO;

/**
 * @author Eric Zhao
 * @since 1.8.0
 */
public class ExceptionCircuitBreaker extends AbstractCircuitBreaker {

    private final int strategy;
    private final int minRequestAmount;
    private final double threshold;

    private final LeapArray<SimpleErrorCounter> stat;

    public ExceptionCircuitBreaker(DegradeRule rule) {
        this(rule, new SimpleErrorCounterLeapArray(1, rule.getStatIntervalMs()));
    }

    ExceptionCircuitBreaker(DegradeRule rule, LeapArray<SimpleErrorCounter> stat) {
        super(rule);
        this.strategy = rule.getGrade();
        boolean modeOk = strategy == DEGRADE_GRADE_EXCEPTION_RATIO || strategy == DEGRADE_GRADE_EXCEPTION_COUNT;
        AssertUtil.isTrue(modeOk, "rule strategy should be error-ratio or error-count");
        AssertUtil.notNull(stat, "stat cannot be null");
        this.minRequestAmount = rule.getMinRequestAmount();
        this.threshold = rule.getCount();
        this.stat = stat;
    }

    @Override
    protected void resetStat() {
        // Reset current bucket (bucket count = 1).
        stat.currentWindow().value().reset();
    }

    @Override
    public void onRequestComplete(Context context) {
        // 获取资源 Entry
        Entry entry = context.getCurEntry();
        if (entry == null) {
            return;
        }
        // 尝试获取 资源中的 异常
        Throwable error = entry.getError();
        // 获取计数器，同样采用了滑动窗口来计数
        SimpleErrorCounter counter = stat.currentWindow().value();
        if (error != null) {
            // 如果出现异常，则 error计数器 +1
            counter.getErrorCount().add(1);
        }
        // 不管是否出现异常，total计数器 +1
        counter.getTotalCount().add(1);

        // 判断异常比例是否超出阈值
        handleStateChangeWhenThresholdExceeded(error);
    }

    private void handleStateChangeWhenThresholdExceeded(Throwable error) {
        // 如果当前已经是OPEN状态，不做处理
        if (currentState.get() == State.OPEN) {
            return;
        }

        // 如果已经是 HALF_OPEN 状态，判断是否需求切换状态
        if (currentState.get() == State.HALF_OPEN) {
            // In detecting request
            if (error == null) {
                // 没有异常，则从 HALF_OPEN 到 CLOSED
                fromHalfOpenToClose();
            } else {
                // 有异常，再次进入OPEN
                fromHalfOpenToOpen(1.0d);
            }
            return;
        }

        // 说明当前是CLOSE状态，需要判断是否触发阈值
        List<SimpleErrorCounter> counters = stat.values();
        long errCount = 0;
        long totalCount = 0;

        // 累加计算 异常请求数量、总请求数量
        for (SimpleErrorCounter counter : counters) {
            errCount += counter.errorCount.sum();
            totalCount += counter.totalCount.sum();
        }

        // 如果总请求数量未达到阈值，什么都不做
        if (totalCount < minRequestAmount) {
            return;
        }
        double curCount = errCount;
        if (strategy == DEGRADE_GRADE_EXCEPTION_RATIO) {
            // Use errorRatio
            // 计算请求的异常比例
            curCount = errCount * 1.0d / totalCount;
        }

        // 如果比例超过阈值，切换到 OPEN
        if (curCount > threshold) {
            transformToOpen(curCount);
        }
    }

    static class SimpleErrorCounter {
        private LongAdder errorCount;
        private LongAdder totalCount;

        public SimpleErrorCounter() {
            this.errorCount = new LongAdder();
            this.totalCount = new LongAdder();
        }

        public LongAdder getErrorCount() {
            return errorCount;
        }

        public LongAdder getTotalCount() {
            return totalCount;
        }

        public SimpleErrorCounter reset() {
            errorCount.reset();
            totalCount.reset();
            return this;
        }

        @Override
        public String toString() {
            return "SimpleErrorCounter{" +
                "errorCount=" + errorCount +
                ", totalCount=" + totalCount +
                '}';
        }
    }

    static class SimpleErrorCounterLeapArray extends LeapArray<SimpleErrorCounter> {

        public SimpleErrorCounterLeapArray(int sampleCount, int intervalInMs) {
            super(sampleCount, intervalInMs);
        }

        @Override
        public SimpleErrorCounter newEmptyBucket(long timeMillis) {
            return new SimpleErrorCounter();
        }

        @Override
        protected WindowWrap<SimpleErrorCounter> resetWindowTo(WindowWrap<SimpleErrorCounter> w, long startTime) {
            // Update the start time and reset value.
            w.resetTo(startTime);
            w.value().reset();
            return w;
        }
    }
}
