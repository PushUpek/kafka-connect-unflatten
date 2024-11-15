/*
 * Copyright © 2019 Christopher Matta (chris.matta@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.pushupek.kafka.connect.smt;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public class Unflatten<R extends ConnectRecord<R>>
    implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Unflatten fields in a record.";

    private static final String FIELD_SEPARATOR_CONFIG = "delimiter";
    private static final String FIELD_SEPARATOR_DEFAULT = ".";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(
            FIELD_SEPARATOR_CONFIG,
            ConfigDef.Type.STRING,
            FIELD_SEPARATOR_DEFAULT,
            ConfigDef.Importance.HIGH,
            "Separator for flattened fields"
        );

    private String fieldSeparator;

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return record;
        }

        if (record.value() instanceof Map) {
            Map<String, Object> value = (Map<String, Object>) record.value();
            Map<String, Object> unflattenedValue = unflatten(value);
            return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                unflattenedValue,
                record.timestamp()
            );
        }

        return record;
    }

    private Map<String, Object> unflatten(Map<String, Object> flattenedMap) {
        Map<String, Object> unflattenedMap = new HashMap<>();
        Pattern separatorPattern = Pattern.compile(
            Pattern.quote(fieldSeparator)
        );

        for (Map.Entry<String, Object> entry : flattenedMap.entrySet()) {
            String[] keys = separatorPattern.split(entry.getKey());
            Map<String, Object> current = unflattenedMap;

            for (int i = 0; i < keys.length - 1; i++) {
                String key = keys[i];
                if (
                    !current.containsKey(key) ||
                    !(current.get(key) instanceof Map)
                ) {
                    current.put(key, new HashMap<>());
                }
                current = (Map<String, Object>) current.get(key);
            }
            current.put(keys[keys.length - 1], entry.getValue());
        }

        return unflattenedMap;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        fieldSeparator = config.getString(FIELD_SEPARATOR_CONFIG);
    }

    @Override
    public void close() {
        // Nothing to do here
    }

    public static class Value<R extends ConnectRecord<R>>
        extends Unflatten<R> {}
}
