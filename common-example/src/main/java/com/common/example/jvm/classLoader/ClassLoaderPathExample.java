package com.common.example.jvm.classLoader;

import sun.misc.Launcher;

import java.net.URL;

/**
 * 功能：ClassLoader 加载路径
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/5 下午11:19
 */
public class ClassLoaderPathExample {

    // 启动类加载器加载的路径
    public static void bootClassLoaderLoadingPath() {
        String bootStrapPath = System.getProperty("sun.boot.class.path");
        System.out.println("启动类加载器加载的路径: ");
        for (String paths : bootStrapPath.split(";")){
            for (String path : paths.split(":")) {
                System.out.println(path);
            }
        }

        System.out.println("启动类加载器加载的路径: ");
        URL[] urLs = Launcher.getBootstrapClassPath().getURLs();
        for (URL url : urLs) {
            System.out.println(url.toExternalForm());
        }
    }

    // 拓展类加载器加载的路径
    public static void extClassLoaderLoadingPath() {
        String extClassLoaderPath = System.getProperty("java.ext.dirs");
        System.out.println("拓展类加载器加载的路径: ");
        for (String paths : extClassLoaderPath.split(";")){
            for (String path : paths.split(":")) {
                System.out.println(path);
            }
        }
    }

    // 应用程序类加载器加载的路径
    public static void appClassLoaderLoadingPath(){
        String appClassLoaderPath = System.getProperty("java.class.path");
        for (String paths : appClassLoaderPath.split(";")){
            System.out.println("应用程序类加载器加载的路径: ");
            for (String path : paths.split(":")) {
                System.out.println(path);
            }
        }
    }

    public static void main(String[] args) {
        // bootClassLoaderLoadingPath();
        // extClassLoaderLoadingPath();
        appClassLoaderLoadingPath();
    }
}
