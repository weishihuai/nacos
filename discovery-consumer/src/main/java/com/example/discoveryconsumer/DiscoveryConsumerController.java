package com.example.discoveryconsumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

@RestController
@RequestMapping("/consumer")
public class DiscoveryConsumerController {

    @Autowired
    private RestTemplate restTemplate;

    @GetMapping(value = "/hello/{text}")
    public String hello(@PathVariable(value = "text") String text) {
        return restTemplate.getForObject("http://discovery-provider/provider/hello/" + text, String.class);
    }
}
