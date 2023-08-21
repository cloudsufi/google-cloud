/*
 * Copyright © 2023 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.gcp.bigquery.sink;

import com.google.cloud.bigquery.TimePartitioning;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Tests for {@link BigQuerySinkConfig}.
 */

public class BigQuerySinkConfigTest {

    @Test
    public void testValidateTimePartitioningColumnWithHourAndDate() throws NoSuchMethodException,
            InvocationTargetException, IllegalAccessException {
        MockFailureCollector collector = new MockFailureCollector();
        BigQuerySinkConfig config = BigQuerySinkConfig.builder().build();
        Method method = config.getClass().getDeclaredMethod("validateTimePartitioningColumn",
                String.class, FailureCollector.class, Schema.class, TimePartitioning.Type.class);
        method.setAccessible(true);

        String columnName = "partitionFrom";
        Schema fieldSchema = Schema.recordOf("test", Schema.Field.of("partitionFrom",
                Schema.of(Schema.LogicalType.DATE)));
        TimePartitioning.Type timePartitioningType = TimePartitioning.Type.HOUR;

        method.invoke(config, columnName, collector, fieldSchema, timePartitioningType);
        Assert.assertEquals(String.format("Partition column '%s' is of invalid type '%s'.",
                        columnName, fieldSchema.getDisplayName()),
                collector.getValidationFailures().stream().findFirst().get().getMessage());
    }

    @Test
    public void testValidateTimePartitioningColumnWithHourAndTimestamp() throws NoSuchMethodException,
            InvocationTargetException, IllegalAccessException {
        MockFailureCollector collector = new MockFailureCollector();
        BigQuerySinkConfig config = BigQuerySinkConfig.builder().build();
        Method method = config.getClass().getDeclaredMethod("validateTimePartitioningColumn",
                String.class, FailureCollector.class, Schema.class, TimePartitioning.Type.class);
        method.setAccessible(true);

        String columnName = "partitionFrom";
        Schema schema = Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);

        Schema fieldSchema = schema.isNullable() ? schema.getNonNullable() : schema;
        TimePartitioning.Type timePartitioningType = TimePartitioning.Type.HOUR;

        method.invoke(config, columnName, collector, fieldSchema, timePartitioningType);
        Assert.assertEquals(0, collector.getValidationFailures().size());
    }

    @Test
    public void testValidateTimePartitioningColumnWithDayAndString() throws NoSuchMethodException,
            InvocationTargetException, IllegalAccessException {
        MockFailureCollector collector = new MockFailureCollector();
        BigQuerySinkConfig config = BigQuerySinkConfig.builder().build();
        Method method = config.getClass().getDeclaredMethod("validateTimePartitioningColumn",
                String.class, FailureCollector.class, Schema.class, TimePartitioning.Type.class);
        method.setAccessible(true);

        String columnName = "partitionFrom";
        Schema schema = Schema.of(Schema.Type.STRING);

        Schema fieldSchema = schema.isNullable() ? schema.getNonNullable() : schema;
        TimePartitioning.Type timePartitioningType = TimePartitioning.Type.DAY;

        method.invoke(config, columnName, collector, fieldSchema, timePartitioningType);
        Assert.assertEquals(String.format("Partition column '%s' is of invalid type '%s'.",
                        columnName, fieldSchema.getDisplayName()),
                collector.getValidationFailures().stream().findFirst().get().getMessage());
    }
}
