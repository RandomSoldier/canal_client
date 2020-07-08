package com.zzy.canal_client.model;

import java.util.Map;

public class DdlReturn {
    public String statement ;  // 语句类型
    public String operation ;  // 操作类型
    public Map map ; // 列及属性

    public DdlReturn setDdlReturn(String statement,String operation,Map map) {
        this.statement = statement;
        this.operation = operation;
        this.map = map;
        return this;
    }
}
