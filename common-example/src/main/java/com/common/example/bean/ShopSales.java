package com.common.example.bean;

import com.google.gson.annotations.SerializedName;

/**
 * 功能：下单金额
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/18 上午8:39
 */
public class ShopSales {
    // 商品Id
    @SerializedName("product_id")
    private Long productId;
    // 类目类型
    private String category;
    // 下单金额
    private Long price;
    // 下单时间
    private Long timestamp;

    public ShopSales() {
    }

    public ShopSales(Long productId, String category, Long price, Long timestamp) {
        this.productId = productId;
        this.category = category;
        this.price = price;
        this.timestamp = timestamp;
    }

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public Long getPrice() {
        return price;
    }

    public void setPrice(Long price) {
        this.price = price;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
