/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dhanuka.kafka.connect.smt;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.fasterxml.jackson.databind.JsonNode;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.RegexValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public class FilterRouterReplicator<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Update the record topic using the configured regular expression and replacement string."
            + "<p/>Under the hood, the regex is compiled to a <code>java.util.regex.Pattern</code>. "
            + "If the pattern matches the input topic, <code>java.util.regex.Matcher#replaceFirst()</code> is used with the replacement string to obtain the new topic.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.REGEX, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new RegexValidator(), ConfigDef.Importance.HIGH,
                    "Regular expression to use for matching.")
            .define(ConfigName.FIELD, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new RegexValidator(), ConfigDef.Importance.HIGH,
                    "Filtering field.")
            .define(ConfigName.REPLACEMENT, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    "Replacement string.");


    private interface ConfigName {
        String REGEX = "regex";
        String REPLACEMENT = "replacement";
        String FIELD = "field";
    }

    private Pattern regex;
    private String replacement; 
    private String field;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        regex = Pattern.compile(config.getString(ConfigName.REGEX));
        replacement = config.getString(ConfigName.REPLACEMENT);
        field = config.getString(ConfigName.FIELD);
    }

    @Override
    public R apply(R record) {
        Object value = record.value();
        if(value != null){
            System.out.println("The class of " + value +" is " + value.getClass().getName());
            JsonNode node = JSONUtils.convertToJSON(value);
            JsonNode nField = node.get(field);
            System.out.println("JSON node " + node.toString() +" Field" + field + " json field "+nField);
            Long fieldValue = nField.asLong();

            
            if(fieldValue < 0){
                final String topic = replacement;
                System.out.println(" Topic : " + topic);
                return record.newRecord(topic, record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), record.value(), record.timestamp());
               /*  final Matcher matcher = regex.matcher(record.topic());
                if (matcher.matches()) {
                    final String topic = matcher.replaceFirst(replacement);
                    System.out.println(" Topic : " + topic);
                    return record.newRecord(topic, record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), record.value(), record.timestamp());
                } */
            }
           
        }
        
        return record;
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

}
