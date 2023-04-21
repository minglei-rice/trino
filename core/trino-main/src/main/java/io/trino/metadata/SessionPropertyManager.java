/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.connector.CatalogHandle;
import io.trino.connector.CatalogName;
import io.trino.connector.CatalogServiceProvider;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.ParameterRewriter;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class SessionPropertyManager
{
    private static final Logger LOG = Logger.get(SessionPropertyManager.class);
    private static final JsonCodecFactory JSON_CODEC_FACTORY = new JsonCodecFactory();
    private final ConcurrentMap<String, PropertyMetadata<?>> systemSessionProperties = new ConcurrentHashMap<>();
    private final CatalogServiceProvider<Map<String, PropertyMetadata<?>>> connectorSessionProperties;
    private final ConcurrentMap<String, String> runtimeSystemSessionProperties = new ConcurrentHashMap<>();
    private final ConcurrentMap<CatalogName, Map<String, String>> runtimeConnectorSessionProperties = new ConcurrentHashMap<>();

    public SessionPropertyManager()
    {
        this(new SystemSessionProperties());
    }

    public SessionPropertyManager(SystemSessionPropertiesProvider systemSessionPropertiesProvider)
    {
        this(ImmutableSet.of(systemSessionPropertiesProvider), connectorName -> ImmutableMap.of());
    }

    public SessionPropertyManager(Set<SystemSessionPropertiesProvider> systemSessionProperties, CatalogServiceProvider<Map<String, PropertyMetadata<?>>> connectorSessionProperties)
    {
        addSystemSessionProperties(systemSessionProperties
                .stream()
                .flatMap(provider -> provider.getSessionProperties().stream())
                .collect(toImmutableList()));
        this.connectorSessionProperties = requireNonNull(connectorSessionProperties, "connectorSessionProperties is null");
    }

    public void addSystemSessionProperties(List<PropertyMetadata<?>> systemSessionProperties)
    {
        systemSessionProperties
                .forEach(this::addSystemSessionProperty);
    }

    public void addSystemSessionProperty(PropertyMetadata<?> sessionProperty)
    {
        requireNonNull(sessionProperty, "sessionProperty is null");
        checkState(systemSessionProperties.put(sessionProperty.getName(), sessionProperty) == null,
                "System session property '%s' are already registered", sessionProperty.getName());
    }

    public Optional<PropertyMetadata<?>> getSystemSessionPropertyMetadata(String name)
    {
        requireNonNull(name, "name is null");

        return Optional.ofNullable(systemSessionProperties.get(name));
    }

    public Optional<PropertyMetadata<?>> getConnectorSessionPropertyMetadata(CatalogHandle catalogHandle, String propertyName)
    {
        requireNonNull(catalogHandle, "catalogHandle is null");
        requireNonNull(propertyName, "propertyName is null");
        Map<String, PropertyMetadata<?>> properties = connectorSessionProperties.getService(catalogHandle);
        return Optional.ofNullable(properties.get(propertyName));
    }

    public Map<String, String> getRuntimeSystemSessionProperties()
    {
        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
        propertiesBuilder.putAll(runtimeSystemSessionProperties);
        return propertiesBuilder.build();
    }

    public Optional<String> getRuntimeSystemSessionProperty(String propertyName)
    {
        requireNonNull(propertyName, "runtime property name is null");

        return Optional.ofNullable(runtimeSystemSessionProperties.get(propertyName));
    }

    public void addRuntimeSystemSessionProperty(String property, String value)
    {
        runtimeSystemSessionProperties.compute(property, (key, oldValue) -> {
            if (oldValue != null) {
                LOG.info(String.format("The old runtime system session property %s will be changed from %s to %s.", key, oldValue, value));
            }
            return value;
        });
    }

    public void removeRuntimeSystemSessionProperty(String property)
    {
        String value = runtimeSystemSessionProperties.remove(property);
        if (value != null) {
            LOG.info(String.format("The runtime system session property %s = %s has been removed.", property, value));
        }
    }

    public void addRuntimeConnectorSessionProperty(CatalogName catalogName, String property, String value)
    {
        requireNonNull(catalogName, "runtime connector session catalogName is null");
        requireNonNull(property, "runtime connector session property is null");
        requireNonNull(value, "runtime connector session value is null");
        runtimeConnectorSessionProperties.putIfAbsent(catalogName, new HashMap<>());

        String oldValue;
        synchronized (runtimeConnectorSessionProperties) {
            oldValue = runtimeConnectorSessionProperties.get(catalogName).put(property, value);
        }

        if (oldValue != null) {
            LOG.info(String.format("The runtime connector session property %s has been changed from %s to %s.", property, oldValue, value));
        }
    }

    public Map<CatalogName, Map<String, String>> getAllRuntimeConnectorSessionProperties()
    {
        ImmutableMap.Builder<CatalogName, Map<String, String>> propertiesBuilder = ImmutableMap.builder();
        synchronized (runtimeConnectorSessionProperties) {
            runtimeConnectorSessionProperties.forEach((catalogName, propertyMap) -> propertiesBuilder.put(catalogName, new HashMap<>(propertyMap)));
        }
        return propertiesBuilder.build();
    }

    public Map<String, String> getRuntimeConnectorSessionProperties(CatalogName catalogName)
    {
        requireNonNull(catalogName, "catalogName is null");
        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
        synchronized (runtimeConnectorSessionProperties) {
            Map<String, String> map = runtimeConnectorSessionProperties.get(catalogName);
            if (map != null) {
                propertiesBuilder.putAll(map);
            }
        }

        return propertiesBuilder.build();
    }

    public Optional<String> getRuntimeConnectorSessionProperty(CatalogName catalogName, String propertyName)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(propertyName, "propertyName is null");
        synchronized (runtimeConnectorSessionProperties) {
            Map<String, String> properties = runtimeConnectorSessionProperties.get(catalogName);
            if (properties == null || properties.isEmpty()) {
                return Optional.empty();
            }

            return Optional.ofNullable(properties.get(propertyName));
        }
    }

    public void removeRuntimeConnectorSessionProperties(CatalogName catalogName)
    {
        runtimeConnectorSessionProperties.remove(catalogName);
        LOG.info(String.format("The runtime connector %s session properties has been removed.", catalogName));
    }

    public void removeRuntimeConnectorSessionProperty(CatalogName catalogName, String propertyName)
    {
        synchronized (runtimeConnectorSessionProperties) {
            if (runtimeConnectorSessionProperties.containsKey(catalogName)) {
                runtimeConnectorSessionProperties.get(catalogName).remove(propertyName);
            }
        }
        LOG.info(String.format("The runtime connector %s session property %s has been removed.", catalogName, propertyName));
    }

    public List<SessionPropertyValue> getAllSessionProperties(Session session, List<CatalogInfo> catalogInfos)
    {
        requireNonNull(session, "session is null");

        ImmutableList.Builder<SessionPropertyValue> sessionPropertyValues = ImmutableList.builder();
        Map<String, String> systemProperties = session.getSystemProperties();
        for (PropertyMetadata<?> property : new TreeMap<>(systemSessionProperties).values()) {
            String defaultValue = firstNonNull(property.getDefaultValue(), "").toString();
            String value = systemProperties.getOrDefault(property.getName(), defaultValue);
            String actualValue = getRuntimeSystemSessionProperty(property.getName()).orElse(value);

            sessionPropertyValues.add(new SessionPropertyValue(
                    actualValue,
                    defaultValue,
                    property.getName(),
                    Optional.empty(),
                    property.getName(),
                    property.getDescription(),
                    property.getSqlType().getDisplayName(),
                    property.isHidden()));
        }

        for (CatalogInfo catalogInfo : catalogInfos) {
            CatalogHandle catalogHandle = catalogInfo.getCatalogHandle();
            String catalogName = catalogInfo.getCatalogName();
            Map<String, String> connectorProperties = session.getCatalogProperties(catalogName);

            for (PropertyMetadata<?> property : new TreeMap<>(connectorSessionProperties.getService(catalogHandle)).values()) {
                String defaultValue = firstNonNull(property.getDefaultValue(), "").toString();
                String value = connectorProperties.getOrDefault(property.getName(), defaultValue);
                String actualValue = getRuntimeConnectorSessionProperty(new CatalogName(catalogName), property.getName()).orElse(value);

                sessionPropertyValues.add(new SessionPropertyValue(
                        actualValue,
                        defaultValue,
                        catalogName + "." + property.getName(),
                        Optional.of(catalogName),
                        property.getName(),
                        property.getDescription(),
                        property.getSqlType().getDisplayName(),
                        property.isHidden()));
            }
        }

        return sessionPropertyValues.build();
    }

    public <T> T decodeSystemPropertyValue(String name, @Nullable String value, Class<T> type)
    {
        PropertyMetadata<?> property = getSystemSessionPropertyMetadata(name)
                .orElseThrow(() -> new TrinoException(INVALID_SESSION_PROPERTY, "Unknown session property " + name));

        return decodePropertyValue(name, value, type, property);
    }

    public <T> T decodeCatalogPropertyValue(CatalogHandle catalogHandle, String catalogName, String propertyName, @Nullable String propertyValue, Class<T> type)
    {
        String fullPropertyName = catalogName + "." + propertyName;
        PropertyMetadata<?> property = getConnectorSessionPropertyMetadata(catalogHandle, propertyName)
                .orElseThrow(() -> new TrinoException(INVALID_SESSION_PROPERTY, "Unknown session property " + fullPropertyName));

        return decodePropertyValue(fullPropertyName, propertyValue, type, property);
    }

    public void validateSystemSessionProperty(String propertyName, String propertyValue)
    {
        PropertyMetadata<?> propertyMetadata = getSystemSessionPropertyMetadata(propertyName)
                .orElseThrow(() -> new TrinoException(INVALID_SESSION_PROPERTY, "Unknown session property " + propertyName));

        decodePropertyValue(propertyName, propertyValue, propertyMetadata.getJavaType(), propertyMetadata);
    }

    public void validateCatalogSessionProperty(String catalogName, CatalogHandle catalogHandle, String propertyName, String propertyValue)
    {
        String fullPropertyName = catalogName + "." + propertyName;
        PropertyMetadata<?> propertyMetadata = getConnectorSessionPropertyMetadata(catalogHandle, propertyName)
                .orElseThrow(() -> new TrinoException(INVALID_SESSION_PROPERTY, "Unknown session property " + fullPropertyName));

        decodePropertyValue(fullPropertyName, propertyValue, propertyMetadata.getJavaType(), propertyMetadata);
    }

    private static <T> T decodePropertyValue(String fullPropertyName, @Nullable String propertyValue, Class<T> type, PropertyMetadata<?> metadata)
    {
        if (metadata.getJavaType() != type) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, format("Property %s is type %s, but requested type was %s", fullPropertyName,
                    metadata.getJavaType().getName(),
                    type.getName()));
        }
        if (propertyValue == null) {
            return type.cast(metadata.getDefaultValue());
        }
        Object objectValue = deserializeSessionProperty(metadata.getSqlType(), propertyValue);
        try {
            return type.cast(metadata.decode(objectValue));
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (Exception e) {
            // the system property decoder can throw any exception
            throw new TrinoException(INVALID_SESSION_PROPERTY, format("%s is invalid: %s", fullPropertyName, propertyValue), e);
        }
    }

    public static Object evaluatePropertyValue(Expression expression, Type expectedType, Session session, PlannerContext plannerContext, AccessControl accessControl, Map<NodeRef<Parameter>, Expression> parameters)
    {
        Expression rewritten = ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(parameters), expression);
        Object value = evaluateConstantExpression(rewritten, expectedType, plannerContext, session, accessControl, parameters);

        // convert to object value type of SQL type
        BlockBuilder blockBuilder = expectedType.createBlockBuilder(null, 1);
        writeNativeValue(expectedType, blockBuilder, value);
        Object objectValue = expectedType.getObjectValue(session.toConnectorSession(), blockBuilder, 0);

        if (objectValue == null) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, "Session property value must not be null");
        }
        return objectValue;
    }

    public static String serializeSessionProperty(Type type, Object value)
    {
        if (value == null) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, "Session property cannot be null");
        }
        if (BooleanType.BOOLEAN.equals(type)) {
            return value.toString();
        }
        if (BigintType.BIGINT.equals(type)) {
            return value.toString();
        }
        if (IntegerType.INTEGER.equals(type)) {
            return value.toString();
        }
        if (DoubleType.DOUBLE.equals(type)) {
            return value.toString();
        }
        if (VarcharType.VARCHAR.equals(type)) {
            return value.toString();
        }
        if (type instanceof ArrayType || type instanceof MapType) {
            return getJsonCodecForType(type).toJson(value);
        }
        throw new TrinoException(INVALID_SESSION_PROPERTY, format("Session property type %s is not supported", type));
    }

    private static Object deserializeSessionProperty(Type type, String value)
    {
        if (value == null) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, "Session property cannot be null");
        }
        if (VarcharType.VARCHAR.equals(type)) {
            return value;
        }
        if (BooleanType.BOOLEAN.equals(type)) {
            return Boolean.valueOf(value);
        }
        if (BigintType.BIGINT.equals(type)) {
            return Long.valueOf(value);
        }
        if (IntegerType.INTEGER.equals(type)) {
            return Integer.valueOf(value);
        }
        if (DoubleType.DOUBLE.equals(type)) {
            return Double.valueOf(value);
        }
        if (type instanceof ArrayType || type instanceof MapType) {
            return getJsonCodecForType(type).fromJson(value);
        }
        throw new TrinoException(INVALID_SESSION_PROPERTY, format("Session property type %s is not supported", type));
    }

    private static <T> JsonCodec<T> getJsonCodecForType(Type type)
    {
        if (VarcharType.VARCHAR.equals(type)) {
            return (JsonCodec<T>) JSON_CODEC_FACTORY.jsonCodec(String.class);
        }
        if (BooleanType.BOOLEAN.equals(type)) {
            return (JsonCodec<T>) JSON_CODEC_FACTORY.jsonCodec(Boolean.class);
        }
        if (BigintType.BIGINT.equals(type)) {
            return (JsonCodec<T>) JSON_CODEC_FACTORY.jsonCodec(Long.class);
        }
        if (IntegerType.INTEGER.equals(type)) {
            return (JsonCodec<T>) JSON_CODEC_FACTORY.jsonCodec(Integer.class);
        }
        if (DoubleType.DOUBLE.equals(type)) {
            return (JsonCodec<T>) JSON_CODEC_FACTORY.jsonCodec(Double.class);
        }
        if (type instanceof ArrayType) {
            Type elementType = ((ArrayType) type).getElementType();
            return (JsonCodec<T>) JSON_CODEC_FACTORY.listJsonCodec(getJsonCodecForType(elementType));
        }
        if (type instanceof MapType) {
            Type keyType = ((MapType) type).getKeyType();
            Type valueType = ((MapType) type).getValueType();
            return (JsonCodec<T>) JSON_CODEC_FACTORY.mapJsonCodec(getMapKeyType(keyType), getJsonCodecForType(valueType));
        }
        throw new TrinoException(INVALID_SESSION_PROPERTY, format("Session property type %s is not supported", type));
    }

    private static Class<?> getMapKeyType(Type type)
    {
        if (VarcharType.VARCHAR.equals(type)) {
            return String.class;
        }
        if (BooleanType.BOOLEAN.equals(type)) {
            return Boolean.class;
        }
        if (BigintType.BIGINT.equals(type)) {
            return Long.class;
        }
        if (IntegerType.INTEGER.equals(type)) {
            return Integer.class;
        }
        if (DoubleType.DOUBLE.equals(type)) {
            return Double.class;
        }
        throw new TrinoException(INVALID_SESSION_PROPERTY, format("Session property map key type %s is not supported", type));
    }

    public static class SessionPropertyValue
    {
        private final String fullyQualifiedName;
        private final Optional<String> catalogName;
        private final String propertyName;
        private final String description;
        private final String type;
        private final String value;
        private final String defaultValue;
        private final boolean hidden;

        private SessionPropertyValue(
                String value,
                String defaultValue,
                String fullyQualifiedName,
                Optional<String> catalogName,
                String propertyName,
                String description,
                String type,
                boolean hidden)
        {
            this.fullyQualifiedName = fullyQualifiedName;
            this.catalogName = catalogName;
            this.propertyName = propertyName;
            this.description = description;
            this.type = type;
            this.value = value;
            this.defaultValue = defaultValue;
            this.hidden = hidden;
        }

        public String getFullyQualifiedName()
        {
            return fullyQualifiedName;
        }

        public Optional<String> getCatalogName()
        {
            return catalogName;
        }

        public String getPropertyName()
        {
            return propertyName;
        }

        public String getDescription()
        {
            return description;
        }

        public String getType()
        {
            return type;
        }

        public String getValue()
        {
            return value;
        }

        public String getDefaultValue()
        {
            return defaultValue;
        }

        public boolean isHidden()
        {
            return hidden;
        }
    }
}
