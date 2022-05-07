package com.common.example.jvm.classLoader;

import java.util.Arrays;
import java.util.List;

/**
 * 功能：ClassLoader 加载路径
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/5 下午11:19
 */
public class ClassLoaderPathExample {

//    ${JAVA_HOME}/jre/lib/resources.jar
//    ${JAVA_HOME}/jre/lib/rt.jar
//    ${JAVA_HOME}/jre/lib/sunrsasign.jar
//    ${JAVA_HOME}/jre/lib/jsse.jar
//    ${JAVA_HOME}/jre/lib/jce.jar
//    ${JAVA_HOME}/jre/lib/charsets.jar
//    ${JAVA_HOME}/jre/lib/jfr.jar
//    ${JAVA_HOME}/jre/classes
    public static void bootClassLoaderLoadingPath() {
        //获取启动列加载器加载的目录
        String bootStrapLoadingPath = System.getProperty("sun.boot.class.path");
        //把加载的目录转为集合
        List<String> bootLoadingPathList = Arrays.asList(bootStrapLoadingPath.split(";"));
        for (String bootPath : bootLoadingPathList){
            System.out.println("启动类加载器加载的目录：" + bootPath);
            for (String path : bootPath.split(":")) {
                System.out.println(path);
            }
        }
    }

    /**
     * 拓展类加载器加载的目录
     */
    public static void extClassLoaderLoadingPath() {
        //获取启动列加载器加载的目录
        String bootStrapLoadingPath = System.getProperty("java.ext.dirs");
        //把加载的目录转为集合
        List<String> bootLoadingPathList = Arrays.asList(bootStrapLoadingPath.split(";"));
        for (String bootPath:bootLoadingPathList){
            System.out.println("拓展类加载器加载的目录："+bootPath);
            for (String path : bootPath.split(":")) {
                System.out.println(path);
            }
        }
    }

    /**
     * 应用程序类加载器加载的目录
     */
    public static void appClassLoaderLoadingPath(){
        //获取启动列加载器加载的目录
        String bootStrapLoadingPath=System.getProperty("java.class.path");
        //把加载的目录转为集合
        List<String> bootLoadingPathList= Arrays.asList(bootStrapLoadingPath.split(";"));
        for (String bootPath:bootLoadingPathList){
            System.out.println("应用程序类加载器加载的目录："+bootPath);
            for (String path : bootPath.split(":")) {
                System.out.println(path);
            }
        }
    }

    public static void main(String[] args) {
        // bootClassLoaderLoadingPath();
        extClassLoaderLoadingPath();
        // appClassLoaderLoadingPath();
    }
}
