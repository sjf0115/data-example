package com.common.example.bean;
/**
 * 功能：订单
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/3/5 下午10:08
 */
public class Order {
    // 订单Id
    private String orderId;
    // 用户Id
    private Integer userId;
    // 订单金额
    private Double amount;
    // 下单时间
    private Long createTime;

    public Order() {
    }

    public Order(String orderId, Integer userId, Double amount, Long createTime) {
        this.orderId = orderId;
        this.userId = userId;
        this.amount = amount;
        this.createTime = createTime;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public Long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", userId=" + userId +
                ", amount=" + amount +
                ", createTime=" + createTime +
                '}';
    }
}
