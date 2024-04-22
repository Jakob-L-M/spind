package runner;

import core.Spind;

import java.io.IOException;

public class Runner {
    public static void main(String[] args) throws IOException {


        for (Config.Dataset d : new Config.Dataset[]{Config.Dataset.TESMA}) {
            for (int i = 0; i < 3; i++) {
                for (Boolean[] filter : new Boolean[][]{new Boolean[]{true, true}, new Boolean[]{true, false}, new Boolean[]{false, false}}) {
                    System.out.println("Starting: " + d.name() + " filter: " + filter[0] + " refining: " + filter[1]);
                    long startTime = System.currentTimeMillis();

                    Config config = new Config(d, 1.0);

                    config.useFilter = filter[0];
                    config.refineFilter = filter[1];

                    config.maxNary = -1;

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
        }
    }
}
