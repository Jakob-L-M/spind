package structures;

import lombok.Getter;
import lombok.Setter;

import java.util.Collection;

/**
 * A PINDList is a single-linked list which stores pIND. The list is associated with some dependant attribute and stores the (open) referenced attributes with the respective
 * remaining violations.
 */
public class PINDList {

    private PINDElement first = null;
    private PINDElement last = null;
    private int size = 0;

    public PINDList(Collection<Integer> seed, int except) {
        initialize(seed, except);
    }

    public PINDList() {

    }

    public int size() {
        return size;
    }

    private void initialize(Collection<Integer> seed, int except) {
        for (int value : seed) {
            if (value != except) {
                this.add(value, 0L);
            }
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

    /**
     * Check if there are still items remaining.
     *
     * @return True if there is no item in the list, False otherwise
     */
    public boolean isEmpty() {
        return this.first == null;
    }

    /**
     * Use this function to iterate over the list and conditionally remove items if necessary.
     *
     * @return an PINDIterator which yields all PINDElements in the list one after another.
     */
    public PINDIterator elementIterator() {
        return new PINDIterator();
    }

    public static class PINDElement {

        public int id;
        public long violationCap;
        @Getter
        private long violations;
        private PINDElement next = null;

        public PINDElement(int value, long maxViolations) {
            this.id = value;
            this.violationCap = maxViolations;
            violations = 0L;
        }

        /**
         * Use this method to reduce the open violations by some amount
         *
         * @param occurrences The number of occurrences which should be subtracted from the open violations.
         * @return the remaining violations
         */
        public long violate(long occurrences) {
            violations += occurrences;
            return violationCap - violations;
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
            // the last element should be removed
            else if (this.next == null) {
                last = this.previous;
                last.next = null;
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