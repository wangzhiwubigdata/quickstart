package org.myorg.quickstart.Table05;

import java.io.Serializable;

/**
 * Created by wangchangye on 2020/4/23.
 */
public class Item implements Serializable{
    private String name;
    private Integer id;

    public Item() {
    }

    public Item(String name, Integer id) {
        this.name = name;
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "Item{" +
                "name='" + name + '\'' +
                ", id=" + id +
                '}';
    }
}
