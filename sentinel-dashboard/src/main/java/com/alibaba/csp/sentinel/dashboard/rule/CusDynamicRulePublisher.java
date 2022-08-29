package com.alibaba.csp.sentinel.dashboard.rule;

/**
 * <p>
 *  通用配置规则保存到配置中心
 * </p>
 *
 * @author KAIMAO.LONG
 * @since 2022-08-29
 */
public interface CusDynamicRulePublisher<T>{


    /**
     * Publish rules to remote rule configuration center for given application name.
     *
     * @param app app name
     * @param rules list of rules to push
     * @throws Exception if some error occurs
     */
    void publish(String app,String dataId, T rules) throws Exception;
}
