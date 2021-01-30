package ru.bmstu.iu9.db.zvoa.dbms.io.impl;

import java.util.Objects;

import io.netty.channel.ChannelHandlerContext;

public class Connection {
    private final ChannelHandlerContext channelHandlerContext;

    public Connection(ChannelHandlerContext channelHandlerContext) {
        this.channelHandlerContext = channelHandlerContext;
    }

    public ChannelHandlerContext getChannelHandlerContext() {
        return channelHandlerContext;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Connection that = (Connection) o;
        return Objects.equals(channelHandlerContext, that.channelHandlerContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(channelHandlerContext);
    }
}