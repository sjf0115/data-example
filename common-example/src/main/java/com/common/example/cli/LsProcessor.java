package com.common.example.cli;

import org.apache.commons.cli.*;

/**
 * 功能：ls 命令行解析
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/12/25 下午8:38
 */
public class LsProcessor {

    private Options options;

    private void parseLs(String[] args) {
        try {
            // 定义
            options = new Options();
            options.addOption("a", "all", false, "do not hide entries starting with .");
            options.addOption("A", "almost-all", false, "do not list implied . and ..");
            options.addOption("b", "escape", false, "print octal escapes for non-graphic characters");
            options.addOption("B", "ignore-backups", false, "do not list implied entries ending with ~");
            options.addOption("c", false, "with -lt: sort by, and show, ctime (time of last "
                    + "modification of file status information) with "
                    + "-l:show ctime and sort by name otherwise: sort "
                    + "by ctime");
            options.addOption("C", false, "list entries by columns");
            options.addOption(Option.builder("SIZE")
                    .longOpt("block-size")
                    .argName("SIZE")
                    .desc("use SIZE-byte blocks")
                    .hasArg()
                    .build());
            // 解析
            CommandLineParser parser = new DefaultParser();
            CommandLine commandLine = parser.parse(options, args);
            // 查询
            if (commandLine.hasOption("block-size")) {
                System.out.println("block-size: " + commandLine.getOptionValue("block-size"));
            }
            if (commandLine.hasOption("a")) {
                System.out.println("a: " + commandLine.hasOption("a"));
            }
            if (commandLine.hasOption("b")) {
                System.out.println("b: " + commandLine.hasOption("b"));
            }
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            printUsage();
        }
    }

    // 打印帮助信息
    private void printUsage() {
        new HelpFormatter().printHelp("hive", options);
    }

    public static void main(String[] args) {
        //args = new String[]{ "--block-size=10", "--all"};
        args = new String[]{"ls", "-ab"};
        LsProcessor example = new LsProcessor();
        example.parseLs(args);
    }
}
