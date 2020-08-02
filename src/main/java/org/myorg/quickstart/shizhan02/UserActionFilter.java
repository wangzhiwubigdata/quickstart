package org.myorg.quickstart.shizhan02;


import org.apache.flink.api.common.functions.FilterFunction;

public class UserActionFilter implements FilterFunction<String> {
    @Override
    public boolean filter(String input) throws Exception {
        return input.contains("CLICK") && input.startsWith("{") && input.endsWith("}");
    }
}
