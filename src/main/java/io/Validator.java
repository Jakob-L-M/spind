package io;

import runner.Config;
import structures.Attribute;
import structures.Candidates;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

public class Validator {
    Config config;
    Attribute[] attributeIndex;
    Candidates candidates;
    Queue<String> topValues;
    Reader[] readers;

    public Validator(Config config, Attribute[] attributeIndex, Candidates candidates) throws IOException {
        this.config = config;
        this.attributeIndex = attributeIndex;
        this.candidates = candidates;
        topValues = new PriorityQueue<>();
        initReaders(attributeIndex);
    }

    public void validate() {

    }

    public void loadNextGroup() {

    }

    public void parseLine() {

    }

    private void initReaders(Attribute[] attributeIndex) throws IOException {
        List<Integer> relations = Arrays.stream(attributeIndex).mapToInt(Attribute::getRelationId).distinct().boxed().toList();
        readers = new BufferedReader[relations.size()];
        for (int i = 0; i < readers.length; i++) {
            readers[i] = Files.newBufferedReader(Path.of(config.tempFolder + File.separator + "relation_" + relations.get(i) + ".txt"));
        }
    }
}
