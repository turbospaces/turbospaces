package org.springframework.samples.jpetstore.domain;

@SuppressWarnings("javadoc")
public class Product {

    /* Private Fields */

    private String productId;
    private String categoryId;
    private String name;
    private String description;

    /* JavaBeans Properties */

    public String getProductId() {
        return productId;
    }

    public void setProductId(final String productId) {
        this.productId = productId.trim();
    }

    public String getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(final String categoryId) {
        this.categoryId = categoryId;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }

    /* Public Methods */

    @Override
    public String toString() {
        return getName();
    }
}
