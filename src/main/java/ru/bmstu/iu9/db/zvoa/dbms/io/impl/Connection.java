package ru.bmstu.iu9.db.zvoa.dbms.io.impl;

import java.util.Objects;

import io.netty.channel.ChannelHandlerContext;

/**
 * The type Connection.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class Connection {
    private final ChannelHandlerContext channelHandlerContext;

    /**
     * Instantiates a new Connection.
     *
     * @param channelHandlerContext the channel handler context
     */
    public Connection(ChannelHandlerContext channelHandlerContext) {
        this.channelHandlerContext = channelHandlerContext;
    }

    /**
     * Gets channel handler context.
     *
     * @return the channel handler context
     */
    public ChannelHandlerContext getChannelHandlerContext() {
        return channelHandlerContext;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        Connection that = (Connection) object;
        return Objects.equals(channelHandlerContext, that.channelHandlerContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(channelHandlerContext);
    }
}