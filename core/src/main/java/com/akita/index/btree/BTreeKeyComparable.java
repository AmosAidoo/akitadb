package com.akita.index.btree;

import com.akita.datatype.AkitaValue;
import com.akita.datatype.AkitaValueComparator;

import java.util.List;

public interface BTreeKeyComparable {
    List<AkitaValue> columns();

    default int compareColumnsTo(BTreeKeyComparable other) {
        List<AkitaValue> a = this.columns();
        List<AkitaValue> b = other.columns();

        if (a.size() != b.size()) {
            throw new IllegalArgumentException(
                    "BTreeKey column count mismatch: " + a.size() + " vs " + b.size()
            );
        }

        AkitaValueComparator comparator = AkitaValueComparator.INSTANCE;
        for (int i = 0; i < a.size(); i++) {
            int res = comparator.compare(a.get(i), b.get(i));
            if (res != 0) return res;
        }

        return 0;
    }
}