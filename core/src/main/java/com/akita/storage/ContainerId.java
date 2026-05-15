package com.akita.storage;

import java.util.Objects;
import java.util.UUID;

/**
 * ContainerId represents the id for physical containers persisted on
 * a storage medium
 */
public class ContainerId implements Comparable<ContainerId> {
    public static final ContainerId MIN = new ContainerId(new UUID(Long.MIN_VALUE, Long.MIN_VALUE));
    public static final ContainerId MAX = new ContainerId(new UUID(Long.MAX_VALUE, Long.MAX_VALUE));

    private final UUID value;

    private ContainerId(UUID value) {
        this.value = value;
    }

    public static ContainerId generate() {
        return new ContainerId(UUID.randomUUID());
    }

    public static ContainerId fromUUID(UUID value) {
        return new ContainerId(value);
    }

    public UUID getValue() {
        return value;
    }

    @Override
    public int compareTo(ContainerId o) {
        return value.compareTo(o.value);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ContainerId that = (ContainerId) o;
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }

    @Override
    public String toString() {
        return value.toString();
    }
}