package com.zzy.canal_client;

import com.zzy.canal_client.service.CanalClientServer;
import com.zzy.canal_client.util.SpringUtil;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {"com.zzy.*"})
public class CanalClientApplication {
    public static void main(String[] args) {
        SpringApplication.run(CanalClientApplication.class, args);
        CanalClientServer canalClientServer = (CanalClientServer) SpringUtil.getBean("canalClientServerImpl");
        canalClientServer.main();
    }
}
