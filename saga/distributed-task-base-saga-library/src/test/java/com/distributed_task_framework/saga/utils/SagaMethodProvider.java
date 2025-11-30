package com.distributed_task_framework.saga.utils;

import com.distributed_task_framework.saga.exceptions.SagaMethodParseException;
import org.junit.jupiter.api.Disabled;

import java.util.List;
import java.util.Map;

@Disabled
public class SagaMethodProvider {

    public void method_double_int(double s, int i) throws SagaMethodParseException {
        System.out.println("test method is invoked");
    }

    public void method_double_array_int_array3(double[] s, int[][][] i) throws SagaMethodParseException {
        System.out.println("test method is invoked");
    }

    public void method_double_array_string(double[] s, String i) throws SagaMethodParseException {
        System.out.println("test method is invoked");
    }

    public Void method_float_array2_string_array2_void(float[][] s, String[][] i) {
        System.out.println("test method is invoked");
        return null;
    }

    public int method_string_list_int(String s, List<Integer> i) throws SagaMethodParseException {
        System.out.println("test method is invoked");
        return 1;
    }

    public Integer method_map_integer_integer(Map<String, List<Integer>> s, Integer i) {
        System.out.println("test method is invoked");
        return 1;
    }

    public String method_string_integer_string(String s, Integer i) throws SagaMethodParseException {
        System.out.println("test method is invoked");
        return "";
    }

    public String[] method_string_integer_string_array(String s, Integer i) {
        System.out.println("test method is invoked");
        return new String[0];
    }

    public String[][] method_string_integer_string_array2(String s, Integer i) throws SagaMethodParseException {
        System.out.println("test method is invoked");
        return new String[0][0];
    }

    public List<String> method_string_integer_list(String s, Integer i) {
        System.out.println("test method is invoked");
        return List.of();
    }

    public Map<String, String> method_string_array2_integer_array_map(String[][] s, Integer[] i) {
        System.out.println("test method is invoked");
        return Map.of();
    }

    public SagaMethodProvider.InternalTest<List<String>, Map<String, String>> method_string_custom_custom(String s, SagaMethodProvider.InternalTest<List<String>, Map<String, String>> i) {
        System.out.println("test method is invoked");
        return null;
    }

    @SuppressWarnings("unchecked")
    public Map<String, String>[] method_map_array() {
        System.out.println("test method is invoked");
        return new Map[0];
    }

    public static class InternalTest<T, U> {
    }
}
