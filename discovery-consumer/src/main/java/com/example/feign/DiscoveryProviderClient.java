package com.example.feign;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@FeignClient(value = "discovery-provider", contextId = "DiscoveryProviderClient")
@RequestMapping("/provider")
public interface DiscoveryProviderClient {

    @RequestMapping(value = "/feign/{text}", method = RequestMethod.GET)
    String feign(@PathVariable(value = "text") String text);
}
