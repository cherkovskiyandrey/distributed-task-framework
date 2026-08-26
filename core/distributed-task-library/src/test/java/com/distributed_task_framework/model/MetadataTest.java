package com.distributed_task_framework.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetadataTest {
    ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void shouldProvideBasicAccessors() {
        //when
        Metadata metadata = Metadata.from(Map.of(
            "traceId", List.of("a", "b"),
            "tenant", List.of("t")
        ));

        //verify
        assertThat(metadata.isEmpty()).isFalse();
        assertThat(metadata.size()).isEqualTo(2);
        assertThat(metadata.containsKey("traceId")).isTrue();
        assertThat(metadata.containsKey("absent")).isFalse();
        assertThat(metadata.get("traceId")).containsExactly("a", "b");
        assertThat(metadata.get("absent")).isEmpty();
        assertThat(metadata.containsValue("traceId", "a")).isTrue();
        assertThat(metadata.containsValue("traceId", "z")).isFalse();
        assertThat(metadata.asMap()).containsOnlyKeys("traceId", "tenant");
    }

    @Test
    void shouldProvideSingleValueAccessors() {
        //when
        Metadata metadata = Metadata.of("traceId", "a").add("traceId", "b");

        //verify
        assertThat(metadata.getSingle("traceId")).contains("a");
        assertThat(metadata.getSingle("absent")).isEmpty();
        assertThat(metadata.getSingleOrThrow("traceId")).isEqualTo("a");
        assertThatThrownBy(() -> metadata.getSingleOrThrow("absent"))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldDeepCopyOnCreation() {
        //when
        Map<String, List<String>> source = new HashMap<>();
        source.put("key", new ArrayList<>(List.of("a")));

        Metadata metadata = Metadata.from(source);
        source.get("key").add("b");
        source.put("other", List.of("c"));

        //verify
        assertThat(metadata.get("key")).containsExactly("a");
        assertThat(metadata.containsKey("other")).isFalse();
    }

    @Test
    void shouldBeImmutable() {
        //when
        Metadata metadata = Metadata.of("key", "a");

        //verify
        assertThatThrownBy(() -> metadata.asMap().put("k", List.of("v")))
            .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> metadata.get("key").add("b"))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldRejectNullArguments() {
        //when
        Metadata metadata = Metadata.of("k", "v");

        //verify
        assertThatThrownBy(() -> Metadata.from(null))
            .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> Metadata.of(null, "v"))
            .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> Metadata.of("k", null))
            .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> metadata.get(null))
            .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> metadata.getSingle(null))
            .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> metadata.containsValue("k", null))
            .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> metadata.add(null, "v"))
            .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> metadata.add("k", (String) null))
            .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> metadata.addAll("k", null))
            .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> metadata.with("k", (String) null))
            .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> metadata.with("k", (Collection<String>) null))
            .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> metadata.withIfAbsent("k", null))
            .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> metadata.remove(null))
            .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> metadata.removeAll(null))
            .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> metadata.putAll(null))
            .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldAddAndKeepOriginalUntouched() {
        //when
        Metadata original = Metadata.of("traceId", "a");

        Metadata updated = original
            .add("traceId", "b")
            .add("tenant", "t1", "t2")
            .addAll("traceId", List.of("c"));

        //verify
        assertThat(original.get("traceId")).containsExactly("a");
        assertThat(updated.get("traceId")).containsExactly("a", "b", "c");
        assertThat(updated.get("tenant")).containsExactly("t1", "t2");
    }

    @Test
    void shouldReplaceValuesWithWith() {
        //when
        Metadata metadata = Metadata.of("traceId", "a").add("traceId", "b");

        Metadata updated = metadata
            .with("traceId", "z")
            .with("tenant", List.of("t1", "t2"));

        //verify
        assertThat(metadata.get("traceId")).containsExactly("a", "b");
        assertThat(updated.get("traceId")).containsExactly("z");
        assertThat(updated.get("tenant")).containsExactly("t1", "t2");
    }

    @Test
    void shouldPutIfAbsent() {
        //when
        Metadata metadata = Metadata.of("traceId", "a");

        Metadata unchanged = metadata.withIfAbsent("traceId", "z");
        Metadata added = metadata.withIfAbsent("tenant", "t");

        //verify
        assertThat(unchanged).isSameAs(metadata);
        assertThat(added.get("tenant")).containsExactly("t");
    }

    @Test
    void shouldRemoveKeys() {
        //when
        Metadata metadata = Metadata.from(Map.of(
            "a", List.of("1"),
            "b", List.of("2"),
            "c", List.of("3")
        ));

        Metadata updated = metadata
            .remove("a")
            .removeAll(List.of("b", "absent"));

        //verify
        assertThat(updated.asMap()).containsOnlyKeys("c");
        assertThat(metadata.size()).isEqualTo(3);
    }

    @Test
    void shouldPutAllAppendingValues() {
        //when
        Metadata metadata = Metadata.of("a", "1");

        Metadata updated = metadata.putAll(Map.of(
            "a", List.of("2"),
            "b", List.of("3")
        ));

        //verify
        assertThat(updated.get("a")).containsExactly("1", "2");
        assertThat(updated.get("b")).containsExactly("3");
    }

    @Test
    void shouldSerializeToPlainMapJsonAndBack() throws Exception {
        //when
        Metadata metadata = Metadata.of("traceId", "a").add("traceId", "b").add("tenant", "t");

        //do
        String json = objectMapper.writeValueAsString(metadata);
        Metadata deserialized = objectMapper.readValue(json, Metadata.class);

        //verify
        assertThat(json).isEqualTo("{\"traceId\":[\"a\",\"b\"],\"tenant\":[\"t\"]}");
        assertThat(deserialized).isEqualTo(metadata);
    }
}
