package com.common.example.template.velocity;

import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * 功能：Velocity 入门示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/4/19 上午7:36
 */
public class HelloVelocity {
    public static void main(String[] args) {
        // 初始化模板引擎
        VelocityEngine ve = new VelocityEngine();
        ve.setProperty(RuntimeConstants.RESOURCE_LOADER, "classpath");
        ve.setProperty("classpath.resource.loader.class", ClasspathResourceLoader.class.getName());
        ve.init();
        // 根据配置文件生成模板
        Template template = ve.getTemplate("velocity/hello.vm");
        // 设置变量
        VelocityContext ctx = new VelocityContext();
        ctx.put("name", "lucy");
        List<String> fruits = new ArrayList();
        fruits.add("apple");
        fruits.add("banana");
        ctx.put("fruits", fruits);
        // 输出
        StringWriter sw = new StringWriter();
        template.merge(ctx, sw);
        System.out.println(sw);
    }
}
