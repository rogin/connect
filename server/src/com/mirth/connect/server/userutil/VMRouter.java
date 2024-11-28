/*
 * Copyright (c) Mirth Corporation. All rights reserved.
 * 
 * http://www.mirthcorp.com
 * 
 * The software in this package is published under the terms of the MPL license a copy of which has
 * been included with this distribution in the LICENSE.txt file.
 */

package com.mirth.connect.server.userutil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mirth.connect.donkey.server.channel.DispatchResult;
import com.mirth.connect.server.controllers.ChannelController;
import com.mirth.connect.server.controllers.ControllerFactory;
import com.mirth.connect.server.controllers.EngineController;
import com.mirth.connect.userutil.Response;
import com.mirth.connect.userutil.Status;
import com.mirth.connect.util.ErrorMessageBuilder;

/**
 * Utility class used to dispatch messages to channels.
 */
public class VMRouter {
    private Logger logger = LogManager.getLogger(getClass());
    private ChannelController channelController = ControllerFactory.getFactory().createChannelController();
    private EngineController engineController = ControllerFactory.getFactory().createEngineController();

    /**
     * Instantiates a VMRouter object.
     */
    public VMRouter() {}

    /**
     * Dispatches a message to a channel, specified by the deployed channel name. If the dispatch
     * fails for any reason (for example, if the target channel is not started), a {@link Response} object
     * with the {@link Status#ERROR} status and the error message will be returned.
     * 
     * @param channelName
     *            The name of the deployed channel to dispatch the message to.
     * @param message
     *            The message to dispatch to the channel.
     * @return The {@link Response} object returned by the channel, if its source connector is configured to
     *         return one.
     */
    public Response routeMessage(String channelName, String message) {
        return routeMessage(channelName, createRawMessage(message, null, null));
    }

    /**
     * Dispatches a message to a channel, specified by the deployed channel name. If the dispatch
     * fails for any reason (for example, if the target channel is not started), a {@link Response} object
     * with the {@link Status#ERROR} status and the error message will be returned.
     * 
     * @param channelName
     *            The name of the deployed channel to dispatch the message to.
     * @param rawMessage
     *            A {@link RawMessage} object to dispatch to the channel.
     * @return The {@link Response} object returned by the channel, if its source connector is configured to
     *         return one.
     */
    public Response routeMessage(String channelName, RawMessage rawMessage) {
        com.mirth.connect.model.Channel channel = channelController.getDeployedChannelByName(channelName);

        if (channel == null) {
            String message = "Could not find channel to route to for channel name: " + channelName;
            logger.error(message);
            return new Response(Status.ERROR, message);
        }

        return routeMessageByChannelId(channel.getId(), rawMessage);
    }

    /**
     * Dispatches a message to a channel, specified by the deployed channel ID. If the dispatch
     * fails for any reason (for example, if the target channel is not started), a {@link Response} object
     * with the {@link Status#ERROR} status and the error message will be returned.
     * 
     * @param channelId
     *            The ID of the deployed channel to dispatch the message to.
     * @param message
     *            The message to dispatch to the channel.
     * @return The {@link Response} object returned by the channel, if its source connector is configured to
     *         return one.
     */
    public Response routeMessageByChannelId(String channelId, String message) {
        return routeMessageByChannelId(channelId, createRawMessage(message, null, null));
    }

    /**
     * Dispatches a message to a channel, specified by the deployed channel ID. If the dispatch
     * fails for any reason (for example, if the target channel is not started), a {@link Response} object
     * with the {@link Status#ERROR} status and the error message will be returned.
     * 
     * @param channelId
     *            The ID of the deployed channel to dispatch the message to.
     * @param rawMessage
     *            A {@link RawMessage} object to dispatch to the channel.
     * @return The {@link Response} object returned by the channel, if its source connector is configured to
     *         return one.
     */
    public Response routeMessageByChannelId(String channelId, RawMessage rawMessage) {
        try {
            DispatchResult dispatchResult = engineController.dispatchRawMessage(channelId, convertRawMessage(rawMessage), false, true);

            Response response = null;
            if (dispatchResult != null && dispatchResult.getSelectedResponse() != null) {
                response = new Response(dispatchResult.getSelectedResponse());
            }

            return response;
        } catch (Throwable e) {
            String message = "Error routing message to channel id: " + channelId;
            logger.error(message, e);
            String responseStatusMessage = ErrorMessageBuilder.buildErrorResponse(message, e);
            String responseError = ErrorMessageBuilder.buildErrorMessage(this.getClass().getSimpleName(), message, e);
            return new Response(Status.ERROR, null, responseStatusMessage, responseError);
        }
    }

    private com.mirth.connect.donkey.model.message.RawMessage convertRawMessage(RawMessage message) {
        if (message.isBinary()) {
            return new com.mirth.connect.donkey.model.message.RawMessage(message.getRawBytes(), message.getDestinationMetaDataIds(), message.getSourceMap());
        } else {
            return new com.mirth.connect.donkey.model.message.RawMessage(message.getRawData(), message.getDestinationMetaDataIds(), message.getSourceMap());
        }
    }

    /**
        Create a {@link RawMessage} with the specified content, sourceMap, and destinationSet.

        @param message - The content of the message to be sent, textual or binary. As String or byte[].
        @param sourceMap - A map containing entries to include in the sourceMap of the {@link RawMessage} (optional).
        @param destinationSet - A collection of integers
            (metadata IDs) representing which destinations to dispatch the message to. Null may be passed to
            indicate all destinations. If unspecified, all destinations is the default (optional).
        @return - A {@link RawMessage} object containing the message, source, and destination information.
    */
    public RawMessage createRawMessage(Object message, Map<String, Object> sourceMap, Collection<Number> destinationSet) {
        if(message instanceof byte[]) {
            return new RawMessage((byte[])message, destinationSet, sourceMap);
        } else {
            return new RawMessage(message.toString(), destinationSet, sourceMap);
        }
    }
}
