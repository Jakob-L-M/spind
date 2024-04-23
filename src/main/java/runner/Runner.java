package runner;

import core.Spind;

import java.io.IOException;

public class Runner {
    public static void main(String[] args) throws IOException {

        long startTime = System.currentTimeMillis();

        Config config = new Config(Config.Dataset.CARS, 1.0);

        config.maxNary = 1;

        if (args.length == 4) {
            config.CHUNK_SIZE = Integer.parseInt(args[0]);
            config.SORT_SIZE = Integer.parseInt(args[1]);
            config.MERGE_SIZE = Integer.parseInt(args[2]);
            config.VALIDATION_SIZE = Integer.parseInt(args[3]);
            System.out.println("Used args to set variables");
        }

        Spind spind = new Spind(config);
        spind.execute();

        System.out.println("Execution took: " + (System.currentTimeMillis() - startTime) + "ms");

    }
}
