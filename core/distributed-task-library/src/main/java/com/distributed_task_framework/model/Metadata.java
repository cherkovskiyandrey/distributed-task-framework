package com.distributed_task_framework.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.springframework.lang.NonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable metadata of a task: a multi-value string map.
 * Serialized to JSON as a plain map: {@code {"key": ["value1", "value2"]}}.
 * All modification methods return a new instance.
 */
@EqualsAndHashCode
@ToString
public class Metadata {
    private static final Metadata EMPTY = new Metadata(Map.of());

    private final Map<String, List<String>> values;

    private Metadata(Map<String, List<String>> values) {
        this.values = copyOf(values);
    }

    public static @NonNull Metadata empty() {
        return EMPTY;
    }

    /**
     * The map is deeply copied, so later changes of the source are not reflected.
     */
    @JsonCreator
    public static @NonNull Metadata from(@NonNull Map<String, List<String>> values) {
        Objects.requireNonNull(values, "values");
        if (values.isEmpty()) {
            return EMPTY;
        }
        return new Metadata(values);
    }

    public static @NonNull Metadata of(@NonNull String key, @NonNull String value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        return new Metadata(Map.of(key, List.of(value)));
    }

    /**
     * @return metadata as an unmodifiable map
     */
    @JsonValue
    public @NonNull Map<String, List<String>> asMap() {
        return values;
    }

    /**
     * @return values for the key or empty list if the key is absent
     */
    public @NonNull List<String> get(@NonNull String key) {
        Objects.requireNonNull(key, "key");
        return values.getOrDefault(key, List.of());
    }

    /**
     * @return first value for the key if present
     */
    public @NonNull Optional<String> getSingle(@NonNull String key) {
        Objects.requireNonNull(key, "key");
        return Optional.ofNullable(values.get(key)).flatMap(valueList -> valueList.stream().findFirst());
    }

    /**
     * @throws IllegalArgumentException if the key is absent or has no values
     */
    public @NonNull String getSingleOrThrow(@NonNull String key) {
        return getSingle(key).orElseThrow(
            () -> new IllegalArgumentException("Metadata: no value for key=[%s]".formatted(key))
        );
    }

    public boolean containsKey(@NonNull String key) {
        Objects.requireNonNull(key, "key");
        return values.containsKey(key);
    }

    public boolean containsValue(@NonNull String key, @NonNull String value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        return values.getOrDefault(key, List.of()).contains(value);
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }

    public int size() {
        return values.size();
    }

    /**
     * Add the value to existing values of the key.
     */
    public @NonNull Metadata add(@NonNull String key, @NonNull String value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        return addAll(key, List.of(value));
    }

    /**
     * Add the values to existing values of the key.
     */
    public @NonNull Metadata add(@NonNull String key, @NonNull String... values) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(values, "values");
        return addAll(key, List.of(values));
    }

    public @NonNull Metadata addAll(@NonNull String key, @NonNull Collection<String> values) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(values, "values");
        Map<String, List<String>> copy = toMutableCopy();
        copy.computeIfAbsent(key, ignored -> new ArrayList<>()).addAll(values);
        return new Metadata(copy);
    }

    /**
     * Set the single value for the key, replacing existing values.
     */
    public @NonNull Metadata with(@NonNull String key, @NonNull String value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        return with(key, List.of(value));
    }

    /**
     * Set the values for the key, replacing existing values.
     */
    public @NonNull Metadata with(@NonNull String key, @NonNull Collection<String> values) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(values, "values");
        Map<String, List<String>> copy = toMutableCopy();
        copy.put(key, new ArrayList<>(values));
        return new Metadata(copy);
    }

    /**
     * Set the value for the key only if the key is absent.
     *
     * @return new metadata or this if the key is already present
     */
    public @NonNull Metadata withIfAbsent(@NonNull String key, @NonNull String value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        return containsKey(key) ? this : with(key, value);
    }

    public @NonNull Metadata remove(@NonNull String key) {
        Objects.requireNonNull(key, "key");
        Map<String, List<String>> copy = toMutableCopy();
        copy.remove(key);
        return new Metadata(copy);
    }

    public @NonNull Metadata removeAll(@NonNull Collection<String> keys) {
        Objects.requireNonNull(keys, "keys");
        Map<String, List<String>> copy = toMutableCopy();
        keys.forEach(copy::remove);
        return new Metadata(copy);
    }

    /**
     * Add all entries of the map, appending values by existing keys.
     */
    public @NonNull Metadata putAll(@NonNull Map<String, List<String>> other) {
        Objects.requireNonNull(other, "other");
        Map<String, List<String>> copy = toMutableCopy();
        other.forEach((key, valuesToAdd) ->
            copy.computeIfAbsent(key, ignored -> new ArrayList<>()).addAll(valuesToAdd)
        );
        return new Metadata(copy);
    }

    private Map<String, List<String>> toMutableCopy() {
        Map<String, List<String>> copy = new LinkedHashMap<>();
        values.forEach((key, valueList) -> copy.put(key, new ArrayList<>(valueList)));
        return copy;
    }

    private static Map<String, List<String>> copyOf(Map<String, List<String>> source) {
        Map<String, List<String>> copy = new LinkedHashMap<>();
        source.forEach((key, valueList) -> copy.put(
            key,
            Collections.unmodifiableList(new ArrayList<>(valueList))
        ));
        return Collections.unmodifiableMap(copy);
    }
}
