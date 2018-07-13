package com.model;

import java.io.Serializable;

public class Emoji implements Serializable{

    public static final long UID = 192830918293L;

    private long count;
    private String emoji;

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String getEmoji() {
        return emoji;
    }

    public void setEmoji(String emoji) {
        this.emoji = emoji;
    }
}
