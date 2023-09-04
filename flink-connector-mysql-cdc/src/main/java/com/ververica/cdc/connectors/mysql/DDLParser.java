package com.ververica.cdc.connectors.mysql;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DDLParser {
    public static String parseTableName(String ddlStatement) {
        String regex = "ALTER\\s+TABLE\\s+(\\w+)";
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(ddlStatement);

        if (matcher.find()) {
            return matcher.group(1);
        } else {
            return null; // DDL statement does not match the expected pattern
        }
    }

    public static String parseOperation(String ddlStatement) {
        String regex = "(ADD|ALTER|DROP)\\s+COLUMN";
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(ddlStatement);

        if (matcher.find()) {
            return matcher.group(1);
        } else {
            return null; // DDL statement does not match the expected pattern
        }
    }

    public static String parseColumn(String ddlStatement) {
        String regex = "COLUMN\\s+(\\w+)";
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(ddlStatement);

        if (matcher.find()) {
            return matcher.group(1);
        } else {
            return null; // DDL statement does not match the expected pattern
        }
    }

    public static String parseDefaultValue(String ddlStatement) {
        String regex = "DEFAULT\\s+(\\w+)";
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(ddlStatement);

        if (matcher.find()) {
            return matcher.group(1);
        } else {
            return null; // DDL statement does not match the expected pattern
        }
    }

    public static String parseComment(String ddlStatement) {
        String regex = "COMMENT\\s+'(.*?)'";
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(ddlStatement);

        if (matcher.find()) {
            return matcher.group(1);
        } else {
            return null; // DDL statement does not match the expected pattern
        }
    }
}
