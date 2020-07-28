package org.myorg.quickstart.topn28;


public class OrderDetail {

    private Long userId; //下单用户id
    private Long itemId; //商品id
    private String citeName;//用户所在城市
    private Double price;//订单金额
    private Long timeStamp;//下单时间

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public String getCiteName() {
        return citeName;
    }

    public void setCiteName(String citeName) {
        this.citeName = citeName;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "OrderDetail{" +
                "userId=" + userId +
                ", itemId=" + itemId +
                ", citeName='" + citeName + '\'' +
                ", price=" + price +
                ", timeStamp=" + timeStamp +
                '}';
    }

    public OrderDetail(Long userId, Long itemId, String citeName, Double price, Long timeStamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.citeName = citeName;
        this.price = price;
        this.timeStamp = timeStamp;
    }
}
