package com.common.example.cli;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.cli.*;

import java.util.Properties;

/**
 * 功能：实现 Hive CLI 选项解析
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/12/23 下午9:24
 */
public class HiveOptionsProcessor {
    private Gson gson = new GsonBuilder().create();
    private final Options options = new Options();
    private CommandLine commandLine;

    @SuppressWarnings("static-access")
    public HiveOptionsProcessor() {
        // 1. 定义阶段
        // -e 'quoted-query-string'
        options.addOption(Option.builder("e")
                .hasArg()
                .argName("quoted-query-string")
                .desc("SQL from command line")
                .build());

        // 自定义变量 -d, --define
        options.addOption(Option.builder()
                .hasArgs()
                .numberOfArgs(2)
                .argName("key=value")
                .valueSeparator('=')
                .longOpt("define")
                .option("d")
                .desc("Variable substitution to apply to Hive commands. e.g. -d A=B or --define A=B")
                .build());

        // 1.3 版本之前
//        options.addOption(OptionBuilder
//                .withValueSeparator()
//                .hasArgs(2)
//                .withArgName("key=value")
//                .withLongOpt("define")
//                .withDescription("Variable substitution to apply to Hive commands. e.g. -d A=B or --define A=B")
//                .create('d'));

        // [-H|--help]
        options.addOption(new Option("H", "help", false, "Print help information"));
        //options.addOption("H", "help", false, "Print help information");

    }

    public boolean process(String[] args) {
        try {
            // 2. 解析阶段
            // CommandLineParser parser = new GnuParser(); // 老版本
            CommandLineParser parser = new DefaultParser();
            commandLine = parser.parse( options, args);

            // 3. 询问阶段
            // -H 或者 --help
            if (commandLine.hasOption('H')) {
                printUsage();
                return true;
            }
            // -d 或者 --define
            Properties hiveVars = commandLine.getOptionProperties("d");
            for (String propKey : hiveVars.stringPropertyNames()) {
                System.out.println("[INFO] -d " + propKey + "=" + hiveVars.getProperty(propKey));
            }
            // -e
            if (commandLine.hasOption('e')) {
                String execString = commandLine.getOptionValue('e');
                System.out.println("[INFO] -e " + execString);
            }

            // 其他测试
            for (String arg : commandLine.getArgList()) {
                System.out.println("ArgList: " + arg);
            }
            System.out.println("Args: " + gson.toJson(commandLine.getArgs()));
            for (Option o : commandLine.getOptions()) {
                System.out.println("Option: " + o.toString());
            }
            System.out.println("OptionProperties: " + gson.toJson(commandLine.getOptionProperties("d")));
            System.out.println("OptionValue: " + gson.toJson(commandLine.getOptionValue("d")));
            System.out.println("OptionValues: " + gson.toJson(commandLine.getOptionValues("d")));
            System.out.println("ParsedOptionValue: " + gson.toJson(commandLine.getParsedOptionValue("d")));

        } catch (ParseException e) {
            System.err.println(e.getMessage());
            printUsage();
            return false;
        }
        return true;
    }

    // 打印帮助信息
    private void printUsage() {
        new HelpFormatter().printHelp("hive", options);
    }

    public static void main(String[] args) {
        HiveOptionsProcessor processor = new HiveOptionsProcessor();
        String[] params = {"-e", "SHOW DATABASES", "-d", "dt=20221001", "--define", "hh=10"};
        //String[] params = {"-H"};
        //String[] params = {"--help"};
        //String[] params = {"--define", "dt=20221001"};
        processor.process(params);
    }
}
