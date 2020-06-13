package com.zengzy.canal_client.service.impl;

import com.alibaba.otter.canal.client.CanalConnector;
import com.zengzy.canal_client.action.AbstractCanalClient;
import com.zengzy.canal_client.service.CanalClientServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * 单机模式的测试例子
 *
 * @author jianghang 2013-4-15 下午04:19:20
 * @version 1.0.4
 */
@Service
public class CanalClientServerImpl implements CanalClientServer {
    protected final static Logger logger = LoggerFactory.getLogger(AbstractCanalClient.class);

    @Resource
    JdbcTemplate jdbcTemplate;

    @Resource
    CanalConnector canalConnector;

    public void main() {

        final AbstractCanalClient clientTest = new AbstractCanalClient();
        clientTest.setConnector(canalConnector);
        clientTest.setTargetConnector(jdbcTemplate);
        clientTest.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {
                try {
                    logger.info("## stop the canal client");
                    clientTest.stop();
                } catch (Throwable e) {
                    logger.warn("##something goes wrong when stopping canal:", e);
                } finally {
                    logger.info("## canal client is down.");
                }
            }

        });
    }
}