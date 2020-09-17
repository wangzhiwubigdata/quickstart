package org.myorg.quickstart.CEP11;

public class ResultPayEvent {

    private Long userId;
    private String type;

    public ResultPayEvent(Long userId, String type) {
        this.userId = userId;
        this.type = type;
    }
}
