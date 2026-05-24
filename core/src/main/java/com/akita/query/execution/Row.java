package com.akita.query.execution;

import com.akita.datatype.AkitaValue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public final class Row implements Iterable<AkitaValue> {
    private final AkitaValue[] values;

    public Row(AkitaValue[] values) {
        this.values = Arrays.copyOf(values, values.length);
    }

    public static Row of(AkitaValue... values) {
        return new Row(values);
    }

    public static Row fromList(List<AkitaValue> values) {
        return new Row(values.toArray(AkitaValue[]::new));
    }

    public AkitaValue get(int ordinalPosition) {
        return values[ordinalPosition];
    }

    public int size() {
        return values.length;
    }

    public List<AkitaValue> values() {
        return List.copyOf(Arrays.asList(values));
    }

    @Override
    public Iterator<AkitaValue> iterator() {
        return new Iterator<>() {
            private int index;

            @Override
            public boolean hasNext() {
                return index < values.length;
            }

            @Override
            public AkitaValue next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return values[index++];
            }
        };
    }
}
