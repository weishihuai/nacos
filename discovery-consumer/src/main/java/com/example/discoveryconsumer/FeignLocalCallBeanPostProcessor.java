package com.example.discoveryconsumer;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.reflect.FieldUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;

public class FeignLocalCallBeanPostProcessor implements BeanPostProcessor, ApplicationContextAware {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private ApplicationContext applicationContext;

    private AtomicBoolean done = new AtomicBoolean(false);

    private String urlPattern = "%s-8080.%s.qdama.abc";

    @Value("${feign.local.domain:}")
    private String localDomain;

    @Value("${feign.local.domain.exclude.services:}")
    private String localDomainExcludeServices;

    @Value("${feign.local.domain.include.services:}")
    private String localDomainIncludeServices;

    @Value("${feign.url.v1.enable:false}")
    private Boolean enableV1;

    @Value("${feign.url.enable:false}")
    private Boolean enable;

    @Value("${feign.url.env:stage01}")
    private String env;


    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (BooleanUtils.isTrue(enable)) {
            return bean;
        }

        if (!done.getAndSet(true)) {
            if (!BooleanUtils.isTrue(enableV1)) {
                return bean;
            }

            final Class beanNameClz;
            try {
                String beanNameOfFeignClientFactoryBean = "org.springframework.cloud.openfeign.FeignClientFactoryBean";
                beanNameClz = Class.forName(beanNameOfFeignClientFactoryBean);
            } catch (ClassNotFoundException e) {
                logger.warn("找不到feign客户端工厂bean类，跳过设置url", e);
                return bean;
            }

            applicationContext.getBeansOfType(beanNameClz).forEach((feignBeanName, beanOfFeignClientFactoryBean) -> {
                try {
                    setUrlFromEnvironmentProps(beanOfFeignClientFactoryBean);
                } catch (Exception e) {
                    logger.warn("feign客户端设置url发生异常", e);
                }
            });
        }

        return bean;
    }

    private void setUrlFromEnvironmentProps(Object obj) throws Exception {
        String serviceName = (String) FieldUtils.readField(obj, "name", true);
        if (StringUtils.isNotBlank(serviceName)) {
            String originUrl = (String) FieldUtils.readField(obj, "url", true);
            if (StringUtils.isNotBlank(originUrl) && originUrl.startsWith("http")) {//已指定url，不做覆盖
                return;
            }

            String url = makeUrl(serviceName);
            FieldUtils.writeField(obj, "url", url, true);
        }
    }

    private String makeUrl(String serviceName) throws IllegalAccessException {
        if (StringUtils.isNotBlank(localDomain) && matchForSetLocalDomain(serviceName)) {
            Field portField = FieldUtils.getField(ServicePortConstants.class, serviceName.replace("-service", "").toUpperCase());
            if (portField != null) {
                Integer port = (Integer) FieldUtils.readStaticField(portField);
                return localDomain + ":" + port;
            }
        }

        return String.format(urlPattern, serviceName, env.trim());
    }

    private boolean matchForSetLocalDomain(String serviceName) {
        String excludes = StringUtils.defaultString(localDomainExcludeServices);
        String includes = StringUtils.defaultString(localDomainIncludeServices);
        return !excludes.contains(serviceName) && (includes.contains("all") || includes.contains(serviceName));
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

}
