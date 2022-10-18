package com.common.example.bean;

/**
 * 功能：下单量
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/18 上午8:39
 */
public class ShopSales {
    // 商品Id
    private Long productId;
    // 类目类型
    private String category;
    // 下单量
    private Long sales;

    public ShopSales() {
    }

    public ShopSales(Long productId, String category, Long sales) {
        this.productId = productId;
        this.category = category;
        this.sales = sales;
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

    public Long getSales() {
        return sales;
    }

    public void setSales(Long sales) {
        this.sales = sales;
    }
}
