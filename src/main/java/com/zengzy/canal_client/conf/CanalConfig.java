package com.zengzy.canal_client.conf;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.net.InetSocketAddress;

/**
 * @author haha
 */
@Configuration
public class CanalConfig {
    // @Value 获取 application.properties配置中端内容
    @Value("${canal.server.ip}")
    private String canalIp;
    @Value("${canal.server.port}")
    private Integer canalPort;
    @Value("${canal.server.destination}")
    private String destination;


    // 获取简单canal-server连接
    @Bean
    @Primary
    public CanalConnector canalConnector() {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress(canalIp, canalPort), destination, "", "");
        return canalConnector;
    }
}