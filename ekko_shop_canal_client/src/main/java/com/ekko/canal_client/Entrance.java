package com.ekko.canal_client;

/**
 * @Author Ekko
 * @Date 2021/2/25 上午 08:46
 * @Version 1.0
 * canal 客户端的入口类
 */
public class Entrance {
    public static void main(String[] args) {
        // 实例化canal的 客户端对象 拉取canalServer的 binlog日志
        CanalClient canalClient = new CanalClient();
        canalClient.start();

    }
}
