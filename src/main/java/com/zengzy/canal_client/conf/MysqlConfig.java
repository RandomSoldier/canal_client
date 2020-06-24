package com.zengzy.canal_client.conf;


import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;



/**
 * @ClassName : MysqlConfig  //类名
 * @Description :   //描述
 * @Author : zengzy  //作者
 * @Date: 2020-06-24 15:22  //时间
 **/
@Component
@ConfigurationProperties(prefix = "spring.datasource.targetmysql")
public class MysqlConfig {
    private String driverClassName;
    private String url;
    private String username;
    private String password;
    private String platform;
    private String type;

    public String getDriverClassName() {
        return driverClassName;
    }

    public MysqlConfig setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public MysqlConfig setUrl(String url) {
        this.url = url;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public MysqlConfig setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public MysqlConfig setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getPlatform() {
        return platform;
    }

    public MysqlConfig setPlatform(String platform) {
        this.platform = platform;
        return this;
    }

    public String getType() {
        return type;
    }

    public MysqlConfig setType(String type) {
        this.type = type;
        return this;
    }

    @Override
    public String toString() {
        return "MysqlConfig{" +
                "driverClassName='" + driverClassName + '\'' +
                ", url='" + url + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", platform='" + platform + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}
