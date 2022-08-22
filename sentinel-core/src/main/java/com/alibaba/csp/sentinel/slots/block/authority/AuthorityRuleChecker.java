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
package com.alibaba.csp.sentinel.slots.block.authority;

import com.alibaba.csp.sentinel.context.Context;
import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.util.StringUtil;

/**
 * Rule checker for white/black list authority.
 *
 * @author Eric Zhao
 * @since 0.2.0
 */
final class AuthorityRuleChecker {

    static boolean passCheck(AuthorityRule rule, Context context) {
        // 得到请求来源 origin
        String requester = context.getOrigin();

        // Empty origin or empty limitApp will pass.
        // 来源为空，或者规则为空，都直接放行
        if (StringUtil.isEmpty(requester) || StringUtil.isEmpty(rule.getLimitApp())) {
            return true;
        }

        // Do exact match with origin name.
        // rule.getLimitApp()得到的就是 白名单 或 黑名单 的字符串
        int pos = rule.getLimitApp().indexOf(requester);
        boolean contain = pos > -1;

        if (contain) {
            // 如果包含 origin，还要进一步做精确判断，把名单列表以","分割，逐个判断
            boolean exactlyMatch = false;
            String[] appArray = rule.getLimitApp().split(",");
            for (String app : appArray) {
                if (requester.equals(app)) {
                    exactlyMatch = true;
                    break;
                }
            }

            contain = exactlyMatch;
        }

        int strategy = rule.getStrategy();
        // 如果是黑名单，并且包含origin，则返回false
        if (strategy == RuleConstant.AUTHORITY_BLACK && contain) {
            return false;
        }

        // 如果是白名单，并且不包含origin，则返回false
        if (strategy == RuleConstant.AUTHORITY_WHITE && !contain) {
            return false;
        }

        // 其它情况返回true
        return true;
    }

    private AuthorityRuleChecker() {}
}
