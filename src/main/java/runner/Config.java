package runner;

import java.io.File;

public class Config {

    public final double threshold;
    public String databaseName;
    public String[] tableNames;
    public String DEFAULT_HEADER_STRING = "column";
    public String folderPath = "D:\\MA\\data" + File.separator;
    public String tempFolder = ".\\temp";
    public String resultFolder = ".\\result";
    public String fileEnding = ".csv";
    public char separator = ',';
    public char quoteChar = '\"';
    public char fileEscape = '\\';
    public boolean strictQuotes = false;
    public boolean ignoreLeadingWhiteSpace = true;
    public boolean inputFileHasHeader = true;
    public boolean inputFileSkipDifferingLines = true; // Skip lines that differ from the dataset's schema
    public String nullString = "";
    public boolean writeResults = true;
    public String executionName = "SPIND";

    public int numThreads = 6;

    public DuplicateHandling duplicateHandling = DuplicateHandling.AWARE;
    public NullHandling nullHandling = NullHandling.SUBSET;

    public Config(Config.Dataset dataset, double threshold) {
        this.setDataset(dataset);
        this.threshold = threshold;
    }

    private void setDataset(Config.Dataset dataset) {
        switch (dataset) {
            case ANIMAL_CROSSING -> {
                this.databaseName = "Kaggle\\animal-crossing-new-horizons-nookplaza-dataset";
                this.tableNames = new String[]{"accessories", "achievements", "art", "bags", "bottoms", "construction",
                        "dress-up", "fencing", "fish", "floors", "fossils", "headwear", "housewares", "insects",
                        "miscellaneous", "music", "other", "photos", "posters", "reactions", "recipes", "rugs", "shoes",
                        "socks", "tools", "tops", "umbrellas", "villagers", "wall-mounted", "wallpaper"};
            }
            case TPCH_1 -> {
                this.databaseName = "TPCH_1";
                this.tableNames = new String[]{"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"};
                this.separator = '|';
                this.inputFileHasHeader = false;
                this.fileEnding = ".tbl";
            }
            case DATA_GOV -> {
                this.databaseName = "data.gov";
                this.tableNames = new String[]{"Air_Quality", "Air_Traffic_Passenger_Statistics", "Crash_Reporting_-_Drivers_Data", "Crime_Data_from_2020_to_Present", "Demographic_Statistics_By_Zip_Code", "diabetes_all_2016", "Electric_Vehicle_Population_Data", "iou_zipcodes_2020", "Lottery_Mega_Millions_Winning_Numbers__Beginning_2002", "Lottery_Powerball_Winning_Numbers__Beginning_2010", "Motor_Vehicle_Collisions_-_Crashes", "National_Obesity_By_State", "NCHS_-_Death_rates_and_life_expectancy_at_birth", "Popular_Baby_Names", "Real_Estate_Sales_2001-2020_GL", "Traffic_Crashes_-_Crashes", "Warehouse_and_Retail_Sales"};
                this.separator = ',';
                this.inputFileHasHeader = true;
                this.fileEnding = ".csv";
            }
            case UEFA -> {
                this.databaseName = "uefa";
                this.tableNames = new String[]{"attacking", "attempts", "defending", "disciplinary", "distributon", "goalkeeping", "goals", "key_stats"};
                this.separator = ',';
                this.inputFileHasHeader = true;
                this.fileEnding = ".csv";
            }
            default -> {
            }
        }
    }

    public enum Dataset {
        TPCH_1, ANIMAL_CROSSING, DATA_GOV, UEFA
    }

    public enum NullHandling {
        SUBSET, FOREIGN, EQUALITY, INEQUALITY
    }

    public enum DuplicateHandling {
        AWARE, UNAWARE
    }
}