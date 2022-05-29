package com.xiaohongshu.agg.common;

public class Event{
    public String user;
    public String url;
    public long eventTime;

    public Event(String user, String url, long eventTime) {
        this.user = user;
        this.url = url;
        this.eventTime = eventTime;
    }




    public Event() {
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }
}