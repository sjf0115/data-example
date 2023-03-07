package com.hive.example.meta.cliDriver;

import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 * 功能：ShutdownHook 示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/1/8 下午6:36
 */
public class ShutdownHookExample {
    public static void main(String[] args) {
        Signal interruptSignal = new Signal("INT");
        SignalHandler oldSignal = Signal.handle(interruptSignal, new SignalHandler() {
            private boolean interruptRequested;
            @Override
            public void handle(Signal signal) {
                boolean initialRequest = !interruptRequested;
                interruptRequested = true;
                if (!initialRequest) {
                    // 第二次 Ctrl+C 其次退出
                    System.out.println("[INFO] 退出...");
                    System.exit(127);
                }
                // 第一次 Ctrl+C 首先杀死正在运行的应用程序
                System.out.println("[INFO] 杀死应用程序...");
            }
        });


    }
}
