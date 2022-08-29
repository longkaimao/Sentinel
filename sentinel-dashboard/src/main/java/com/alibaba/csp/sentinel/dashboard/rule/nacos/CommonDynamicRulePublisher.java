package com.alibaba.csp.sentinel.dashboard.rule.nacos;

import com.alibaba.csp.sentinel.dashboard.rule.CusDynamicRulePublisher;
import com.alibaba.csp.sentinel.util.AssertUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.api.config.ConfigService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * <p>
 *  通用配置规则保存到配置中心
 * </p>
 *
 * @author KAIMAO.LONG
 * @since 2022-08-29
 */
@Component
public class CommonDynamicRulePublisher<T> implements CusDynamicRulePublisher<T> {
    @Value("${nacos.group}")
    private String group;

    @Autowired
    private ConfigService configService;

    @Override
    public void publish(String app, String dataId,T rules) throws Exception {
        AssertUtil.notEmpty(app, "app name cannot be empty");
        if (rules == null) {
            return;
        }

        configService.publishConfig(app + dataId,
                group, JSON.toJSONString(rules));
    }
}
