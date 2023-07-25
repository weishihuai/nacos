package com.example.discoveryprovider;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/provider")
public class DiscoveryProviderController {

    @RequestMapping(value = "/hello/{text}", method = RequestMethod.GET)
    public String hello(@PathVariable(value = "text") String text) {
        return "Nacos is coming :" + text;
    }

    @RequestMapping(value = "/feign/{text}", method = RequestMethod.GET)
    public String feign(@PathVariable(value = "text") String text) {
        return "OpenFeign is coming :" + text;
    }

}
