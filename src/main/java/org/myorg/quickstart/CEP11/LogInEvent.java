package org.myorg.quickstart.CEP11;


public class LogInEvent {

    private Long userId;
    private String isSuccess;
    private Long timeStamp;

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getIsSuccess() {
        return isSuccess;
    }

    public void setIsSuccess(String isSuccess) {
        this.isSuccess = isSuccess;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }


    public LogInEvent(Long userId, String isSuccess, Long timeStamp) {
        this.userId = userId;
        this.isSuccess = isSuccess;
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "LogInEvent{" +
                "userId=" + userId +
                ", isSuccess='" + isSuccess + '\'' +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
