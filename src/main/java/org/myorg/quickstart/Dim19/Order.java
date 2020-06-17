package org.myorg.quickstart.Dim19;

public class Order {
    private Integer cityId;
    private String userName;
    private String items;
    private String cityName;

    public Order(Integer cityId, String userName, String items, String cityName) {
        this.cityId = cityId;
        this.userName = userName;
        this.items = items;
        this.cityName = cityName;
    }

    public Order() {
    }

    public Integer getCityId() {
        return cityId;
    }

    public void setCityId(Integer cityId) {
        this.cityId = cityId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getItems() {
        return items;
    }

    public void setItems(String items) {
        this.items = items;
    }

    public String getCityName() {
        return cityName;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    @Override
    public String toString() {
        return "Order{" +
                "cityId=" + cityId +
                ", userName='" + userName + '\'' +
                ", items='" + items + '\'' +
                ", cityName='" + cityName + '\'' +
                '}';
    }
}
