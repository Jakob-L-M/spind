package structures;

import java.util.ArrayList;
import java.util.List;

public class Metrics {
    public int chunkFiles;
    public int sortFiles;
    public int mergeFiles;
    public int nary;
    public int unary;
    public List<Integer> layerAttributes;
    public List<Integer> layerCandidates;
    public List<Integer> layerPINDs;


    public Metrics() {
        layerAttributes = new ArrayList<>();
        layerCandidates = new ArrayList<>();
        layerPINDs = new ArrayList<>();
    }
}