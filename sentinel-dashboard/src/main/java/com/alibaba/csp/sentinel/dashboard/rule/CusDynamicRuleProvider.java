package com.alibaba.csp.sentinel.dashboard.rule;


/**
 * <p>
 *  通用从配置中心获取配置规则
 * </p>
 *
 * @author KAIMAO.LONG
 * @since 2022-08-29
 */
public interface CusDynamicRuleProvider<T>{


    /**
     * Publish rules to remote rule configuration center for given application name.
     *
     * @param appName app name
     * @throws Exception if some error occurs
     */
    T getRules(String appName, String dataId,Class clazz) throws Exception;
}
