package com.common.example.jvm.classLoader;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * 功能：自定义 ClassLoader
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/8 下午3:48
 */
public class CustomClassLoader extends ClassLoader{

    private String path;
    public CustomClassLoader(String path) {
        this.path = path;
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        System.out.println("CustomClassLoader: " + name);
        try {
            String fileName = path + "/" + name.substring(name.lastIndexOf(".") + 1) + ".class";
            FileInputStream inputStream = new FileInputStream(fileName);
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
}
