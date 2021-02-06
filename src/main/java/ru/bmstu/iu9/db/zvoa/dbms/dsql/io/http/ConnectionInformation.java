package ru.bmstu.iu9.db.zvoa.dbms.dsql.io.http;

import java.util.Objects;

import io.netty.channel.ChannelHandlerContext;

import javax.validation.constraints.NotNull;

/**
 * The Connection - information about Http Connection.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class ConnectionInformation {
    @NotNull
    private final ChannelHandlerContext channelHandlerContext;

    /**
     * Instantiates a new Connection Information for Http channel.
     *
     * @param channelHandlerContext the channel handler context
     */
    public ConnectionInformation(@NotNull ChannelHandlerContext channelHandlerContext) {
        this.channelHandlerContext = channelHandlerContext;
    }

    /**
     * Gets channel handler context.
     *
     * @return the channel handler context
     */
    @NotNull
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
        ConnectionInformation that = (ConnectionInformation) object;
        return Objects.equals(channelHandlerContext, that.channelHandlerContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(channelHandlerContext);
    }
}