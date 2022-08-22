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
package com.alibaba.csp.sentinel.slots.block.flow.controller;

import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.csp.sentinel.slots.block.flow.TrafficShapingController;

import com.alibaba.csp.sentinel.util.TimeUtil;
import com.alibaba.csp.sentinel.node.Node;

/**
 * @author jialiang.linjl
 */
public class RateLimiterController implements TrafficShapingController {

    private final int maxQueueingTimeMs;
    private final double count;

    private final AtomicLong latestPassedTime = new AtomicLong(-1);

    public RateLimiterController(int timeOut, double count) {
        this.maxQueueingTimeMs = timeOut;
        this.count = count;
    }

    @Override
    public boolean canPass(Node node, int acquireCount) {
        return canPass(node, acquireCount, false);
    }

    @Override
    public boolean canPass(Node node, int acquireCount, boolean prioritized) {
        // Pass when acquire count is less or equal than 0.
        if (acquireCount <= 0) {
            return true;
        }
        // Reject when count is less or equal than 0.
        // Otherwise,the costTime will be max of long and waitTime will overflow in some cases.
        // 阈值小于等于 0 ，阻止请求
        if (count <= 0) {
            return false;
        }

        // 获取当前时间
        long currentTime = TimeUtil.currentTimeMillis();
        // Calculate the interval between every two requests.
        // 计算两次请求之间允许的最小时间间隔
        long costTime = Math.round(1.0 * (acquireCount) / count * 1000);

        // Expected pass time of this request.
        // 计算本次请求 允许执行的时间点 = 最近一次请求的可执行时间 + 两次请求的最小间隔
        long expectedTime = costTime + latestPassedTime.get();

        // 如果允许执行的时间点小于当前时间，说明可以立即执行
        if (expectedTime <= currentTime) {
            // Contention may exist here, but it's okay.
            // 更新上一次的请求的执行时间
            latestPassedTime.set(currentTime);
            return true;
        } else {
            // 不能立即执行，需要计算 预期等待时长
            // 预期等待时长 = 两次请求的最小间隔 +最近一次请求的可执行时间 - 当前时间
            // Calculate the time to wait.
            long waitTime = costTime + latestPassedTime.get() - TimeUtil.currentTimeMillis();
            // 如果预期等待时间超出阈值，则拒绝请求
            if (waitTime > maxQueueingTimeMs) {
                return false;
            } else {
                // 预期等待时间小于阈值，更新最近一次请求的可执行时间，加上costTime
                long oldTime = latestPassedTime.addAndGet(costTime);
                try {
                    // 保险起见，再判断一次预期等待时间，是否超过阈值
                    waitTime = oldTime - TimeUtil.currentTimeMillis();
                    if (waitTime > maxQueueingTimeMs) {
                        // 如果超过，则把刚才 加 的时间再 减回来
                        latestPassedTime.addAndGet(-costTime);
                        // 拒绝
                        return false;
                    }
                    // in race condition waitTime may <= 0
                    if (waitTime > 0) {
                        // 预期等待时间在阈值范围内，休眠要等待的时间，醒来后继续执行
                        Thread.sleep(waitTime);
                    }
                    return true;
                } catch (InterruptedException e) {
                }
            }
        }
        return false;
    }

}
