package com.akita.buffer.replacers.arc;

import java.util.Objects;

class ArcList {
    private ArcListItem head = null;
    private ArcListItem tail = null;
    private int length = 0;

    private ArcList() {}

    public static ArcList create() {
        return new ArcList();
    }

    public void removeFromList(ArcListItem node) {
        Objects.requireNonNull(node);
        var prev = node.getPrev();
        var next = node.getNext();
        if (prev != null) {
            prev.setNext(next);
        } else {
            head = next;
        }
        if (next != null) {
            next.setPrev(prev);
        } else {
            tail = prev;
        }
        node.setPrev(null);
        node.setNext(null);
        length--;
    }

    public ArcListItem lruRemove() {
        if (tail == null) {
            return null;
        }
        ArcListItem lru = tail;
        while (lru != null && !lru.getIsEvictable()) {
            lru = lru.getPrev();
        }
        if (lru == null)
            return null;
        removeFromList(lru);
        return lru;
    }

    public ArcListItem removeTail() {
        if (tail == null) {
            return null;
        }
        ArcListItem last = tail;
        ArcListItem prev = tail.getPrev();
        if (prev == null) {
            head = null;
            tail = null;
        } else {
            prev.setNext(null);
            tail = prev;
        }
        last.setPrev(null);
        last.setNext(null);
        length--;
        return last;
    }

    public ArcListItem lruPeek() {
        if (tail == null) {
            return null;
        }
        ArcListItem lru = tail;
        while (lru != null && !lru.getIsEvictable()) {
            lru = lru.getPrev();
        }
        return lru;
    }

    public void mruInsert(ArcListItem node) {
        Objects.requireNonNull(node);
        node.setPrev(null);
        node.setNext(null);
        if (head == null) {
            head = node;
            tail = node;
        } else {
            node.setNext(head);
            head.setPrev(node);
            head = node;
        }
        length++;
    }

    public int size() {
        return length;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("head -> ");
        ArcListItem node = head;
        while (node != null) {
            sb.append(node);
            sb.append(" -> ");
            node = node.getNext();
        }
        sb.append("tail");
        return sb.toString();
    }
}
