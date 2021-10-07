package com.common.example.generics;

import java.util.ArrayList;
import java.util.List;

/**
 * 功能：泛型类型擦除示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/6 下午2:10
 */
public class TypeErasureExample {

    private static void test1() {
        // 字符串列表
        ArrayList<String> strList = new ArrayList<String>();
        strList.add("abc");

        // 数值型列表
        ArrayList<Integer> intList = new ArrayList<Integer>();
        intList.add(123);

        // strList: class java.util.ArrayList
        System.out.println("strList: " + strList.getClass());
        // intList: class java.util.ArrayList
        System.out.println("intList: " + intList.getClass());
        // true
        System.out.println(strList.getClass() == intList.getClass());
    }

    private static void test2() {
        // 目标是定义一个只保存正方形的列表
        List squareList = new ArrayList();
        squareList.add(new Square(4));
        squareList.add(new Square(2));
        // 误添加一个长方形
        squareList.add(new Rectangle(4, 3));
        for (Object square : squareList) {
            Square s = (Square) square; // throw ClassCastException
            System.out.println(s.getArea());
        }
    }

    private static void test3() {
        List<Square> squareList = new ArrayList();
        squareList.add(new Square(4));
        squareList.add(new Square(2));
        //squareList.add(new Rectangle(4, 3)); // error
        for (Object square : squareList) {
            Square s = (Square) square;
            System.out.println(s.getArea());
        }
    }

    private static void test4() throws Exception {
        List<Integer> list = new ArrayList<Integer>();
        // 直接调用 add 方法只能存储整形，因为泛型实际类型为 Integer
        list.add(1);
        // 反射方式调用 add 方法可以存储字符串
        list.getClass().getMethod("add", Object.class).invoke(list, "abc");
        for (int i = 0; i < list.size(); i++) {
            System.out.println(list.get(i));
        }
    }

    private static void test5() {
        List<Integer> list = new ArrayList<Integer>();
        // true
        if(list instanceof ArrayList) {
            System.out.println("true");
        }
        // list instanceof ArrayList<Integer> error
    }

    private static void test6() {
        StrPair strPair = new StrPair();
        strPair.setValue("abc");
        //strPair.setValue(new Object()); //编译错误
    }

    private static void test7() {
//        Object[] stringLists = new List<String>[2];  // compiler error, but pretend it's allowed
//        stringLists[0] = new ArrayList<String>();   // OK
//        stringLists[1] = new ArrayList<Integer>();  // An ArrayStoreException should be thrown,
        // but the runtime can't detect it.
    }

    public static void main(String[] args) throws Exception {
        test5();
    }
}
