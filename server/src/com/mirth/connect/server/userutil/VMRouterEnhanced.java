package com.mirth.connect.server.userutil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mirth.connect.userutil.Response;

public class VMRouterEnhanced extends VMRouter {
    private String channelId;
    private Long messageId;
    private SourceMap envSourceMap;

    public VMRouterEnhanced(String channelId, Long messageId, SourceMap sourceMap) {
        this.channelId = channelId != null ? channelId : "NONE";
        this.messageId = messageId != null ? messageId : -1L;
        this.envSourceMap = sourceMap;
    }

    /**
        If this is called from a context where there is no messageId, a value of -1 will be used.
        <p>
        If this is called from a context where there is no channelId, a value of "NONE" will be used.
    */
    @Override
    public RawMessage createRawMessage(Object message, Map<String, Object> sourceMap, Collection<Number> destinationSet) {
        if (sourceMap == null) {
            sourceMap = java.util.Collections.emptyMap();
        }

        List<String> sourceChannelIds = envLookupAsList("sourceChannelIds", "sourceChannelId");
        List<String> sourceMessageIds = envLookupAsList("sourceMessageIds", "sourceMessageId");

        HashMap<String,Object> newSourceMap = new java.util.HashMap<String,Object>(sourceMap);
        String channelId = this.channelId;
        Long messageId = this.messageId;

        sourceChannelIds.add(channelId);
        sourceMessageIds.add(messageId.toString());

        newSourceMap.put("sourceChannelIds", sourceChannelIds);
        newSourceMap.put("sourceChannelId", channelId);
        newSourceMap.put("sourceMessageIds", sourceMessageIds);
        newSourceMap.put("sourceMessageId", messageId);

        return super.createRawMessage(message, newSourceMap, destinationSet);
    }

    /**
     * Given the specified lookup keys, return the first non-null value as a List.
     * The expectation is the first lookup will return a List, while the second does not.
     * 
     * @param primary
     * @param secondary
     * @return a List containing the first non-null lookup value, else an empty List
     */
    private List<String> envLookupAsList(String primary, String secondary) {
        List<String> result = new ArrayList<String>();

        Object primaryValue = lookupInEnvSourceMap(primary);

        if(primaryValue != null) {
            if(primaryValue instanceof Collection) {
                ((Collection<?>)primaryValue).stream()
                .map(i -> i.toString())
                .forEach(result::add);
            } else if(primaryValue instanceof Object[]) {
                Arrays.stream((Object[])primaryValue)
                .map(i -> i.toString())
                .forEach(result::add);
            }
            //a quicker, riskier option...
            //result.addAll((List<String>)primaryValue);
        } else {
            Object secondaryValue = lookupInEnvSourceMap(secondary);
            if(secondaryValue != null) {
                result.add(secondaryValue.toString());
            }
        }

        return result;
    }

    /**
     * Look up a value from the environment's {@link SourceMap}
     * @param key
     * @return its mapped value, can be null
     */
    private Object lookupInEnvSourceMap(String key) {
        return this.envSourceMap.get(key);
    }

    /**
        Calls to {@link #routeMessage(String, Object, Map, Collection)}

        @see #routeMessage(String, Object, Map, Collection)
    */
    public Response routeMessage(String channelName, Object message, Map<String, Object> sourceMap) {
        return routeMessage(channelName, message, sourceMap, null);
    }

    /**
        Route a message to the specified channelName. Information about the chain of source channel Ids and
        source message Ids will be included in the sourceMap of the downstream message automatically in a
        similar manner as if a Channel Writer was being used.

        @param channelName - The name of the channel to which to route the message.
        @param message - The content of the message to be sent, textual or binary. As String or byte[].
        @param sourceMap - A map containing entries to include in the sourceMap of the sent message.
        @param destinationSet - A collection of integers
            (metadata IDs) representing which destinations to dispatch the message to. Null may be passed to
            indicate all destinations. If unspecified, all destinations is the default.
        @return - The {@link Response} object returned by the channel.

        @see VMRouter#routeMessage(String, RawMessage)
    */
    public Response routeMessage(String channelName, Object message, Map<String, Object> sourceMap, Collection<Number> destinationSet) {
        return super.routeMessage(channelName, createRawMessage(message, sourceMap, destinationSet));
    }

    /**
        Calls to #routeMessageByChannelId(String, Object, Map, Collection)}
        @see #routeMessageByChannelId(String, Object, Map, Collection)
    */
    public Response routeMessageByChannelId(String channelId, Object message) {
        return routeMessageByChannelId(channelId, message, null, null);
    }

    /**
        Calls to #routeMessageByChannelId(String, Object, Map, Collection)}
        @see #routeMessageByChannelId(String, Object, Map, Collection)
    */
    public Response routeMessageByChannelId(String channelId, Object message, Map<String, Object> sourceMap) {
        return routeMessageByChannelId(channelId, message, sourceMap, null);
    }

    /**
        Route a message to the specified channelId. Information about the chain of source channel Ids and
        source message Ids will be included in the sourceMap of the downstream message automatically in a
        similar manner as if a Channel Writer was being used.

        @param channelId - The unique identifier of the channel to which to route the message.
        @param message - The content of the message to be sent, textual or binary. As String or byte[].
        @param sourceMap - A map containing entries to include in the sourceMap of the sent message.
        @param destinationSet - A collection of integers
            (metadata IDs) representing which destinations to dispatch the message to. Null may be passed to
            indicate all destinations. If unspecified, all destinations is the default.
        @return - The {@link Response} object returned by the channel.

        @see {@link VMRouter#routeMessageByChannelId(String, RawMessage)}
    */
    public Response routeMessageByChannelId(String channelId, Object message, Map<String, Object> sourceMap, Collection<Number> destinationSet) {
        return super.routeMessageByChannelId(channelId, createRawMessage(message, sourceMap, destinationSet));
    }
}
