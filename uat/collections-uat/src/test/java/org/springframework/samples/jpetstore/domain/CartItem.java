package org.springframework.samples.jpetstore.domain;

@SuppressWarnings("javadoc")
public class CartItem {

    /* Private Fields */

    private Item item;
    private int quantity;
    private boolean inStock;

    /* JavaBeans Properties */

    public boolean isInStock() {
        return inStock;
    }

    public void setInStock(final boolean inStock) {
        this.inStock = inStock;
    }

    public Item getItem() {
        return item;
    }

    public void setItem(final Item item) {
        this.item = item;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(final int quantity) {
        this.quantity = quantity;
    }

    public double getTotalPrice() {
        if ( item != null )
            return item.getListPrice() * quantity;
        return 0;
    }

    /* Public methods */

    public void incrementQuantity() {
        quantity++;
    }

}
