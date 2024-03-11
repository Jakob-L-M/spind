package structures;

import java.util.Collection;

public class PINDList {

    private int except;
    private PINDElement first = null;
    private PINDElement last = null;
    private Collection<Integer> seed;
    private long initialViolations;
    private int size = 0;

    public PINDList(long initialViolations, Collection<Integer> seed, int except) {
        this.seed = seed;
        this.except = except;
        this.initialViolations = initialViolations;
        initialize();
    }

    public PINDList() {

    }

    public int size() {
        return size;
    }

    private void initialize() {
        if (this.seed != null) {
            for (int value : this.seed)
                if (value != this.except)
                    this.add(value, initialViolations);
            this.seed = null;
        }
    }

    public void add(int value, long violationsLeft) {
        PINDElement element = new PINDElement(value, violationsLeft);
        if (this.last == null) {
            this.first = element;
        } else {
            this.last.next = element;
        }
        this.last = element;
        size++;
    }

    public boolean isEmpty() {
        return this.first == null;
    }

    public PINDIterator elementIterator() {
        return new PINDIterator();
    }

    public static class PINDElement {

        public int referenced;
        public long violationsLeft;
        public PINDElement next = null;

        public PINDElement(int value, long violationsLeft) {
            this.referenced = value;
            this.violationsLeft = violationsLeft;
        }

        public long violate(long occurrences) {
            violationsLeft -= occurrences;
            return violationsLeft;
        }
    }

    public class PINDIterator {

        private PINDElement previous = null;
        private PINDElement current = null;
        private PINDElement next;

        public PINDIterator() {
            this.next = first;
        }

        public boolean hasNext() {
            return this.next != null;
        }

        public PINDElement next() {
            this.previous = this.current;
            this.current = this.next;
            if (this.current != null) {
                this.next = this.current.next;
            }
            assert this.current != null;
            return this.current;
        }

        public void remove() {
            // if there is no previous element, we simply need to point the first pointer of the List to the next entry.
            if (this.previous == null) {
                // point first to next element
                first = this.next;
                // set current to null, since it is 'deleted'
                current = null;
            }
            // if we are at the first or later entry we need to put the next pointer of the previous element to the next element.
            // This means we exclude the current element.
            else {
                // set the previous point to the next element
                this.previous.next = this.next;
                // set the current element to the previous, such that the previous will still be the previous after the next call of next()
                this.current = this.previous;
            }
            size--;
        }
    }
}