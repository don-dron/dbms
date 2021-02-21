/*
 * Copyright (c) 2021 Zvorygin Andrey BMSTU IU-9 https://github.com/don-dron
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
 * OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT
 * OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package ru.bmstu.iu9.db.zvoa.dbms.dsql.io.http;

import java.util.Objects;

import io.netty.channel.ChannelHandlerContext;
import org.jetbrains.annotations.NotNull;

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