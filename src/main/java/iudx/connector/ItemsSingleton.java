package iudx.connector;

import com.google.common.io.LittleEndianDataInputStream;

import java.util.HashSet;
import java.util.Set;

public class ItemsSingleton {

    Set<String> items, itemGroups;
    public static ItemsSingleton itemsSingleton=null;
    private ItemsSingleton(){
        this.items=new HashSet<>();
        this.itemGroups=new HashSet<>();
    }

    public Set<String> getItems() {
        return items;
    }

    public void setItems(Set<String> items) {
        this.items = items;
    }

    public Set<String> getItemGroups() {
        return itemGroups;
    }

    public void setItemGroups(Set<String> itemsGroup) {
        this.itemGroups = itemsGroup;
    }

    public static ItemsSingleton getInstance(){

        if(itemsSingleton == null)
            itemsSingleton=new ItemsSingleton();
        return itemsSingleton;
    }
}
