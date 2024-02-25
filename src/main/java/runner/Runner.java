package runner;

import core.Spind;

import java.io.IOException;

public class Runner {
    public static void main(String[] args) throws IOException {

        long startTime = System.currentTimeMillis();
        Config config = new Config(Config.Dataset.TPCH_1, 1.0);

        Spind spind = new Spind(config);
        spind.execute();
        System.out.println("Execution took: " + (System.currentTimeMillis() - startTime) + "ms");
    }
}
