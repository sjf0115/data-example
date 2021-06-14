package com.common.example.test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * 功能：
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/6/5 下午6:03
 */
public class SubParentLevel {

    private static Map<String, List<String>> parentMap;

    private static void dfs (String parentId, List<String> children, List<String> path) {
        for (String childId : children) {
            List<String> subChildren = parentMap.get(childId);
            path.add(0, childId);
            System.out.println(childId + ", " + parentId + ", " + path.toString());
            dfs(childId, subChildren, path);
            path.remove(0);
        }
    }

    public static void main(String[] args) {
        // parent_id child_id
        parentMap = Maps.newHashMap();
        List<String> list1 = Lists.newArrayList("b", "c", "d", "f");
        parentMap.put("a", list1);

        List<String> list2 = Lists.newArrayList("e");
        parentMap.put("b", list2);

        List<String> list3 = Lists.newArrayList("f", "g");
        parentMap.put("c", list3);

        List<String> list4 = Lists.newArrayList();
        parentMap.put("d", list4);

        List<String> list5 = Lists.newArrayList();
        parentMap.put("e", list5);

        List<String> list6 = Lists.newArrayList();
        parentMap.put("f", list6);

        List<String> list7 = Lists.newArrayList();
        parentMap.put("g", list7);

        List<String> path = Lists.newArrayList();
        for(String parentId : parentMap.keySet()){
            List<String> children = parentMap.get(parentId);
            path.add(parentId);
            dfs(parentId, children, path);
            path.remove(parentId);
        }//for
    }
}
