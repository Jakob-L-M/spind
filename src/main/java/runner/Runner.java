package runner;

import core.Spind;

import java.io.IOException;

public class Runner {
    public static void main(String[] args) throws IOException {

        Config config = new Config(Config.Dataset.UEFA, 1.0);

        Spind spind = new Spind(config);
        spind.execute();
    }
}
