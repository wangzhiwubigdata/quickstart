package org.myorg.quickstart.shizhan02;


public class UserClick {

    private String userId;
    private Long timestamp;
    private String action;

    public UserClick() {

    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public UserClick(String userId, Long timestamp, String action) {
        this.userId = userId;
        this.timestamp = timestamp;
        this.action = action;
    }
}

enum UserAction{
    //点击
    CLICK("CLICK"),
    //购买
    PURCHASE("PURCHASE"),
    //其他
    OTHER("OTHER");

    private String action;
    UserAction(String action) {
        this.action = action;
    }
}



