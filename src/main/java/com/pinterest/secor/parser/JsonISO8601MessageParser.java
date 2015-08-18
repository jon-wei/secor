/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.parser;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import java.util.Calendar;
import javax.xml.bind.DatatypeConverter;

/**
 * JsonISO8601MessageParser extracts timestamp field (specified by 'message.timestamp.name')
 * from JSON data and partitions data by date.
 * timestamp should be expressed in in ISO8601 format.
 */
public class JsonISO8601MessageParser extends TimestampedMessageParser {
    public JsonISO8601MessageParser(SecorConfig config) {
        super(config);
    }

    @Override
    public long extractTimestampMillis(final Message message) {
        JSONObject jsonObject = null;
        try {
            jsonObject = (JSONObject) JSONValue.parse(message.getPayload());
        } catch(ClassCastException cce) {
            System.out.println("Could not cast msg to JSON obj: " + message);
            return 0;
        }
        if (jsonObject != null) {
            Object fieldValue = jsonObject.get(mConfig.getMessageTimestampName());
            if (fieldValue != null) {
                Calendar tsCal = DatatypeConverter.parseDateTime(fieldValue.toString());
                return tsCal.getTimeInMillis();
            }
        }
        return 0;
    }

}
