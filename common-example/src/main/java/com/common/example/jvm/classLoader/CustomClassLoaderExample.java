package com.common.example.jvm.classLoader;

import java.io.IOException;
import java.io.InputStream;

/**
 * 功能：自定义 ClassLoader 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/7 下午11:01
 */
public class CustomClassLoaderExample {

    private static void print() {
        System.out.println("this is a custom classLoader example");
    }

    private static ClassLoader myClassLoader = new ClassLoader() {
        @Override
        protected Class<?> findClass(String name) throws ClassNotFoundException {
            System.out.println(this + " findClass");
            try {
                String fileName = name.substring(name.lastIndexOf(".") + 1) + ".class";
                InputStream inputStream = getClass().getResourceAsStream(fileName);
                if (inputStream == null) {
                    return super.findClass(name);
                }
                byte[] bytes = new byte[inputStream.available()];
                inputStream.read(bytes);
                return defineClass(name, bytes, 0, bytes.length);
            } catch (IOException ex) {
                throw new ClassNotFoundException(name, ex);
            }
        }
    };

    public static void main(String[] args) throws ClassNotFoundException {
        Object cl = myClassLoader.loadClass("com.common.example.jvm.classLoader.CustomClassLoaderExample");
        System.out.println(cl instanceof CustomClassLoaderExample);
    }
}
