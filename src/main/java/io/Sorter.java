package io;

/**
 * The sorter is responsible for the creation of one pre-processed file per table. The files consist of as many lines as
 * there are unique values in the whole relational input. The lines are ordered lexicographically by the value which
 * they are associated with. Further each line carries information in which attribute (Combinations) the value is
 * present and also how often it is present in these.
 */
public class Sorter {

    /**
     * Produces a preprocessed file which overwrites the input file.
     * @param input The relational input which should be processed.
     */
    public void process(RelationalInput input) {

    }

    /**
     * Will spill the current state to disk and clean the used memory
     */
    private void spill() {

    }

    /**
     * Overwrites the input file with the preprocessed structure
     */
    private void writeOutput() {

    }
}
