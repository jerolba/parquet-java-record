package com.jerolba.carpet.impl.read;

import java.util.HashMap;
import java.util.Map;

public class MapHolder {

    public Map<Object, Object> map;

    public void start() {
        map = new HashMap<>();
    }

    public Object end() {
        return map;
    }

    public void put(Object key, Object value) {
        this.map.put(key, value);
    }

}