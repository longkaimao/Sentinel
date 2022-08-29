package com.alibaba.csp.sentinel.dashboard.rule.nacos;

import com.alibaba.csp.sentinel.dashboard.rule.CusDynamicRuleProvider;
import com.alibaba.csp.sentinel.util.StringUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.api.config.ConfigService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * <p>
 *  通用从配置中心获取配置规则
 * </p>
 *
 * @author KAIMAO.LONG
 * @since 2022-08-29
 */
@Component
public class CommonRuleNacosProvider<T> implements CusDynamicRuleProvider<List<T>> {
    @Value("${nacos.group}")
    private String group;
    @Autowired
    private ConfigService configService;

    @Override
    public List<T> getRules(String appName,String dataId,Class clazz) throws Exception {
        String rules = getRules(appName,dataId);
        return JSON.parseArray(rules,clazz);
    }

    private String getRules(String appName, String dataId) throws Exception {
        String rules = configService.getConfig(appName + dataId,
                group, 3000);
        if (StringUtil.isEmpty(rules)) {
            return null;
        }
        return rules;
    }
}
