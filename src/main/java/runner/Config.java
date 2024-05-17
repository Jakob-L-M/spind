package runner;

public class Config {

    public final double threshold;
    public int PARALLEL = Runtime.getRuntime().availableProcessors(); // or specify your desired parallelism level
    public int VALIDATION_SIZE = 250_000;
    public int MERGE_SIZE = 1_200;
    public int CHUNK_SIZE = 6_000_000;
    public int SORT_SIZE = 25_000_000;
    public int maxNary = -1;
    public String databaseName;
    public String[] tableNames;
    public String DEFAULT_HEADER_STRING = "column";
    public String folderPath = "D:\\MA\\data\\";
    public String tempFolder = "E:\\temp";
    public String resultFolder = ".\\statistics";
    public String fileEnding = ".csv";
    public char separator = ',';
    public char quoteChar = '\"';
    public char fileEscape = '\\';
    public boolean strictQuotes = false;
    public boolean ignoreLeadingWhiteSpace = true;
    public boolean inputFileHasHeader = true;
    public boolean inputFileSkipDifferingLines = true; // Skip lines that differ from the dataset's schema
    public String nullString = "";
    public boolean writeResults = false;
    public String executionName = "SPIND";

    public DuplicateHandling duplicateHandling = DuplicateHandling.AWARE;
    public NullHandling nullHandling = NullHandling.SUBSET;

    public boolean refineFilter = true; // whether the bloom filter should be reconstructed in every layer
    public boolean useFilter = true; // whether the bloom filter should be used

    public Config(double threshold) {
        this.threshold = threshold;
    }

    void setDataset(Config.Dataset dataset) {
        switch (dataset) {
            case ANIMAL_CROSSING -> {
                this.databaseName = "ACNH";
                this.tableNames = new String[]{"accessories", "achievements", "art", "bags", "bottoms", "construction", "dress-up", "fencing", "fish", "floors", "fossils",
                        "headwear", "housewares", "insects", "miscellaneous", "music", "other", "photos", "posters", "reactions", "recipes", "rugs", "shoes", "socks", "tools",
                        "tops", "umbrellas", "villagers", "wall-mounted", "wallpaper"};
            }
            case TPCH_1 -> {
                this.databaseName = "TPC-H 1";
                this.tableNames = new String[]{"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"};
                this.separator = '|';
                this.inputFileHasHeader = false;
                this.fileEnding = ".tbl";
            }
            case TPCH_10 -> {
                this.databaseName = "TPC-H 10";
                this.tableNames = new String[]{"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"};
                this.separator = '|';
                this.inputFileHasHeader = false;
                this.fileEnding = ".tbl";
            }
            case POPULATION -> {
                this.databaseName = "Population";
                this.tableNames = new String[]{"midyear_population_age_country_code"};
            }
            case DATA_GOV -> {
                this.databaseName = "US";
                this.tableNames = new String[]{"Air_Quality", "Air_Traffic_Passenger_Statistics", "Crash_Reporting_-_Drivers_Data", "Crime_Data_from_2020_to_Present",
                        "diabetes_all_2016", "Electric_Vehicle_Population_Data", "iou_zipcodes_2020", "Lottery_Mega_Millions_Winning_Numbers__Beginning_2002",
                        "Lottery_Powerball_Winning_Numbers__Beginning_2010", "Motor_Vehicle_Collisions_-_Crashes", "National_Obesity_By_State", "NCHS_" +
                        "-_Death_rates_and_life_expectancy_at_birth", "Popular_Baby_Names", "Real_Estate_Sales_2001-2020_GL", "Traffic_Crashes_-_Crashes",
                        "Warehouse_and_Retail_Sales"};
                this.separator = ',';
                this.inputFileHasHeader = true;
                this.fileEnding = ".csv";
            }
            case CARS -> {
                this.databaseName = "Cars";
                this.tableNames = new String[]{"audi", "bmw", "cclass", "focus", "ford", "hyundi", "merc", "skoda", "toyota", "unclean cclass", "unclean focus", "vauxhall", "vw"};
            }
            case MUSICBRAINZ -> {
                this.databaseName = "Musicbrainz";
                this.tableNames = new String[]{"alternative_release_type", "area", "area_alias", "area_alias_type", "area_gid_redirect", "area_type", "artist", "artist_alias",
                        "artist_alias_type", "artist_credit", "artist_credit_gid_redirect", "artist_credit_name", "artist_gid_redirect", "artist_ipi", "artist_isni",
                        "artist_type", "cdtoc", "country_area", "editor_collection_type", "event", "event_alias", "event_alias_type", "event_gid_redirect", "event_type", "gender"
                        , "genre", "genre_alias", "genre_alias_type", "instrument", "instrument_alias", "instrument_alias_type", "instrument_gid_redirect", "instrument_type",
                        "iso_3166_1", "iso_3166_2", "iso_3166_3", "isrc", "iswc", "label", "label_alias", "label_alias_type", "label_gid_redirect", "label_ipi", "label_isni",
                        "label_type", "language", "link", "link_attribute", "link_attribute_credit", "link_attribute_text_value", "link_attribute_type",
                        "link_creditable_attribute_type", "link_text_attribute_type", "link_type", "link_type_attribute_type", "l_area_area", "l_area_event", "l_area_genre",
                        "l_area_instrument", "l_area_label", "l_area_recording", "l_area_release", "l_area_series", "l_area_url", "l_area_work", "l_artist_artist",
                        "l_artist_event", "l_artist_instrument", "l_artist_label", "l_artist_place", "l_artist_recording", "l_artist_release", "l_artist_release_group",
                        "l_artist_series", "l_artist_url", "l_artist_work", "l_event_event", "l_event_label", "l_event_place", "l_event_recording", "l_event_release",
                        "l_event_release_group", "l_event_series", "l_event_url", "l_event_work", "l_genre_genre", "l_genre_instrument", "l_genre_url", "l_instrument_instrument"
                        , "l_instrument_label", "l_instrument_url", "l_label_label", "l_label_place", "l_label_recording", "l_label_release", "l_label_release_group",
                        "l_label_series", "l_label_url", "l_label_work", "l_place_place", "l_place_recording", "l_place_release", "l_place_series", "l_place_url", "l_place_work"
                        , "l_recording_recording", "l_recording_release", "l_recording_series", "l_recording_url", "l_recording_work", "l_release_group_release_group",
                        "l_release_group_series", "l_release_group_url", "l_release_release", "l_release_series", "l_release_url", "l_series_series", "l_series_url",
                        "l_series_work", "l_url_work", "l_work_work", "medium", "medium_cdtoc", "medium_format", "orderable_link_type", "place", "place_alias", "place_alias_type"
                        , "place_gid_redirect", "place_type", "recording", "recording_alias", "recording_alias_type", "recording_gid_redirect", "release", "release_alias",
                        "release_alias_type", "release_country", "release_gid_redirect", "release_group", "release_group_alias", "release_group_alias_type",
                        "release_group_gid_redirect", "release_group_primary_type", "release_group_secondary_type", "release_group_secondary_type_join", "release_label",
                        "release_packaging", "release_status", "release_unknown_country", "replication_control", "script", "series", "series_alias", "series_alias_type",
                        "series_gid_redirect", "series_ordering_type", "series_type", "track", "track_gid_redirect", "url", "url_gid_redirect", "work", "work_alias",
                        "work_alias_type", "work_attribute", "work_attribute_type", "work_attribute_type_allowed_value", "work_gid_redirect", "work_language", "work_type"};
                this.separator = '\t';
                this.inputFileHasHeader = false;
                this.fileEnding = "";
                this.quoteChar = '\0';
                this.nullString = "\\N";
            }
            case ENSEMBL_UNIPROT -> {
                this.databaseName = "UniProt";
                this.separator = '\t';
                this.fileEnding = ".111.uniprot.tsv";
                this.tableNames = new String[]{"Acanthochromis_polyacanthus.ASM210954v1", "Accipiter_nisus.Accipiter_nisus_ver1.0", "Ailuropoda_melanoleuca.ASM200744v2",
                        "Amphilophus_citrinellus.Midas_v5", "Amphiprion_ocellaris.AmpOce1.0", "Amphiprion_percula.Nemo_v1", "Anabas_testudineus.fAnaTes1.2",
                        "Anas_platyrhynchos" + ".ASM874695v1", "Anas_platyrhynchos_platyrhynchos.CAU_duck1.0", "Anas_zonorhyncha.ASM222487v1", "Anolis_carolinensis.AnoCar2.0v2",
                        "Anser_brachyrhynchus" + ".ASM259213v1", "Anser_cygnoides.GooseV1.0", "Aotus_nancymaae.Anan_2.0", "Astatotilapia_calliptera.fAstCal1.2",
                        "Astyanax_mexicanus.Astyanax_mexicanus-2.0", "Astyanax_mexicanus_pachon.Astyanax_mexicanus-1.0.2", "Bison_bison_bison.Bison_UMD1.0", "Bos_grunniens" +
                        ".LU_Bosgru_v3.0", "Bos_indicus_hybrid" + ".UOA_Brahman_1", "Bos_mutus.BosGru_v2.0", "Bos_taurus.ARS-UCD1.2", "Bos_taurus.ARS-UCD1.3", "Bos_taurus_hybrid"
                        + ".UOA_Angus_1", "Caenorhabditis_elegans" + ".WBcel235", "Callithrix_jacchus.mCalJac1.pat.X", "Callorhinchus_milii.Callorhinchus_milii-6.1.3",
                        "Camarhynchus_parvulus" + ".Camarhynchus_parvulus_V1.1", "Camelus_dromedarius.CamDro2", "Canis_lupus_familiaris.ROS_Cfam_1.0",
                        "Canis_lupus_familiarisbasenji.Basenji_breed-1.1", "Canis_lupus_familiarisboxer" + ".Dog10K_Boxer_Tasha",
                        "Canis_lupus_familiarisgreatdane.UMICH_Zoey_3" + ".1", "Canis_lupus_familiarisgsd.UU_Cfam_GSD_1.0", "Capra_hircus.ARS1", "Capra_hircus_blackbengal" +
                        ".CVASU_BBG_1.0", "Carassius_auratus.ASM336829v1", "Carlito_syrichta.Tarsius_syrichta-2.0.1", "Castor_canadensis.C" + ".can_genome_v1.0",
                        "Cavia_aperea" + ".CavAp1.0", "Cavia_porcellus.Cavpor3.0", "Cebus_imitator.Cebus_imitator-1.0", "Cercocebus_atys.Caty_1.0", "Chelydra_serpentina" +
                        ".Chelydra_serpentina-1.0", "Chinchilla_lanigera.ChiLan1.0", "Chlorocebus_sabaeus.ChlSab1.1", "Choloepus_hoffmanni.choHof1", "Chrysemys_picta_bellii" +
                        ".Chrysemys_picta_bellii-3.0.3", "Ciona_intestinalis.KH", "Ciona_savignyi.CSAV2.0", "Clupea_harengus.Ch_v2.0.2", "Colobus_angolensis_palliatus.Cang.pa_1" +
                        ".0", "Corvus_moneduloides.bCorMon1" + ".pri", "Cottoperca_gobio" + ".fCotGob3.1", "Coturnix_japonica" + ".Coturnix_japonica_2" + ".0",
                        "Cricetulus_griseus_chok1gshd.CHOK1GS_HDv1", "Cricetulus_griseus_crigri.CriGri_1.0", "Cricetulus_griseus_picr.CriGri-PICRH-1.0", "Crocodylus_porosus" +
                        ".CroPor_comp1", "Cyclopterus_lumpus.fCycLum1.pri", "Cynoglossus_semilaevis.Cse_v1.0", "Cyprinodon_variegatus.C_variegatus-1.0", "Cyprinus_carpio_carpio" +
                        ".Cypcar_WagV4.0", "Cyprinus_carpio_germanmirror" + ".German_Mirror_carp_1.0", "Cyprinus_carpio_hebaored.Hebao_red_carp_1.0", "Cyprinus_carpio_huanghe" +
                        ".Hunaghe_carp_2.0", "Danio_rerio.GRCz11", "Dasypus_novemcinctus.Dasnov3.0", "Delphinapterus_leucas.ASM228892v3", "Dicentrarchus_labrax" + ".dlabrax2021"
                        , "Dipodomys_ordii.Dord_2.0", "Dromaius_novaehollandiae.droNov1", "Drosophila_melanogaster.BDGP6.46", "Echinops_telfairi" + ".TENREC",
                        "Electrophorus_electricus.Ee_SOAP_WITH_SSPACE", "Eptatretus_burgeri.Eburgeri_3.2", "Equus_asinus.ASM1607732v2", "Equus_caballus.EquCab3.0",
                        "Erinaceus_europaeus.HEDGEHOG", "Erythrura_gouldiae" + ".GouldianFinch", "Esox_lucius.Eluc_v4", "Felis_catus.Felis_catus_9.0", "Ficedula_albicollis" +
                        ".FicAlb1" + ".5", "Fukomys_damarensis.DMR_v1.0", "Fundulus_heteroclitus.Fundulus_heteroclitus-3.0" + ".2", "Gadus_morhua.gadMor3.0", "Gallus_gallus" +
                        ".bGalGal1.mat" + ".broiler" + ".GRCg7b", "Gallus_gallus_gca000002315v5.GRCg6a", "Gallus_gallus_gca016700215v2.bGalGal1.pat.whiteleghornlayer.GRCg7w",
                        "Gambusia_affinis" + ".ASM309773v1", "Gasterosteus_aculeatus.BROADS1", "Geospiza_fortis.GeoFor_1.0", "Gopherus_agassizii.ASM289641v1", "Gorilla_gorilla" +
                        ".gorGor4", "Haplochromis_burtoni" + ".AstBur1.0", "Heterocephalus_glaber_female.Naked_mole-rat_maternal", "Heterocephalus_glaber_male" +
                        ".Naked_mole-rat_paternal", "Hippocampus_comes" + ".H_comes_QL1_v1", "Homo_sapiens.GRCh38", "Hucho_hucho.ASM331708v1", "Ictalurus_punctatus.IpCoco_1.2",
                        "Ictidomys_tridecemlineatus" + ".SpeTri2.0", "Jaculus_jaculus.JacJac1" + ".0", "Kryptolebias_marmoratus.ASM164957v1", "Labrus_bergylta.BallGen_V1",
                        "Larimichthys_crocea.L_crocea_2.0", "Lates_calcarifer.ASB_HGAPassembly_v1", "Latimeria_chalumnae.LatCha1", "Lepisosteus_oculatus.LepOcu1",
                        "Lonchura_striata_domestica.LonStrDom1", "Loxodonta_africana.loxAfr3", "Lynx_canadensis" + ".mLynCan4_v1.p", "Macaca_fascicularis.Macaca_fascicularis_6" +
                        ".0", "Macaca_mulatta.Mmul_10", "Macaca_nemestrina.Mnem_1.0", "Manacus_vitellinus.ASM171598v2", "Mandrillus_leucophaeus.Mleu.le_1.0",
                        "Mastacembelus_armatus.fMasArm1.2", "Maylandia_zebra" + ".M_zebra_UMD2a", "Meleagris_gallopavo.Turkey_5.1", "Melopsittacus_undulatus.bMelUnd1.mat.Z",
                        "Meriones_unguiculatus.MunDraft-v1.0", "Mesocricetus_auratus" + ".MesAur1.0", "Microcebus_murinus.Mmur_3.0", "Microtus_ochrogaster.MicOch1.0", "Mola_mola" +
                        ".ASM169857v1", "Monodelphis_domestica" + ".ASM229v1", "Monodon_monoceros.NGI_Narwhal_1", "Monopterus_albus" + ".M_albus_1.0", "Moschus_moschiferus" +
                        ".MosMos_v2_BIUU_UCD", "Mustela_putorius_furo" + ".MusPutFur1.0", "Mus_caroli.CAROLI_EIJ_v1.1", "Mus_musculus.GRCm39", "Mus_musculus_129s1svimj" +
                        ".129S1_SvImJ_v1", "Mus_musculus_aj.A_J_v1", "Mus_musculus_akrj.AKR_J_v1", "Mus_musculus_balbcj.BALB_cJ_v1", "Mus_musculus_c3hhej" + ".C3H_HeJ_v1",
                        "Mus_musculus_c57bl6nj.C57BL_6NJ_v1", "Mus_musculus_casteij.CAST_EiJ_v1", "Mus_musculus_cbaj.CBA_J_v1", "Mus_musculus_dba2j.DBA_2J_v1",
                        "Mus_musculus_fvbnj.FVB_NJ_v1", "Mus_musculus_lpj" + ".LP_J_v1", "Mus_musculus_nodshiltj.NOD_ShiLtJ_v1", "Mus_musculus_nzohlltj.NZO_HlLtJ_v1",
                        "Mus_musculus_pwkphj.PWK_PhJ_v1", "Mus_musculus_wsbeij" + ".WSB_EiJ_v1", "Mus_pahari.PAHARI_EIJ_v1.1", "Mus_spicilegus.MUSP714", "Mus_spretus" +
                        ".SPRET_EiJ_v1", "Myotis_lucifugus.Myoluc2.0", "Nannospalax_galili.S" + ".galili_v1.0", "Neolamprologus_brichardi.NeoBri1.0", "Neovison_vison.NNQGG.v01",
                        "Nomascus_leucogenys" + ".Nleu_3.0", "Notamacropus_eugenii.Meug_1.0", "Notechis_scutatus.TS10Xv2-PRI", "Nothobranchius_furzeri.Nfu_20140520",
                        "Numida_meleagris.NumMel1.0", "Ochotona_princeps.OchPri2.0-Ens", "Octodon_degus" + ".OctDeg1.0", "Oncorhynchus_kisutch.Okis_V2", "Oncorhynchus_mykiss" +
                        ".USDA_OmykA_1.1", "Oncorhynchus_tshawytscha.Otsh_v1.0", "Oreochromis_aureus" + ".ASM587006v1", "Oreochromis_niloticus.O_niloticus_UMD_NMBU",
                        "Ornithorhynchus_anatinus" + ".mOrnAna1.p.v1", "Oryctolagus_cuniculus.OryCun2.0", "Oryzias_javanicus.OJAV_1.1", "Oryzias_latipes.ASM223467v1",
                        "Oryzias_latipes_hni" + ".ASM223471v1", "Oryzias_latipes_hsok.ASM223469v1", "Oryzias_melastigma.Om_v0.7.RACA", "Oryzias_sinensis.ASM858656v1",
                        "Otolemur_garnettii.OtoGar3", "Ovis_aries.Oar_v3" + ".1", "Ovis_aries_rambouillet" + ".ARS-UI_Ramb_v2.0", "Ovis_aries_rambouillet.Oar_rambouillet_v1.0",
                        "Panthera_leo.PanLeo1.0", "Panthera_pardus.PanPar1.0", "Panthera_tigris_altaica" + ".PanTig1.0", "Pan_paniscus.panpan1.1", "Pan_troglodytes.Pan_tro_3.0",
                        "Papio_anubis.Panubis1.0", "Paramormyrops_kingsleyae" + ".PKINGS_0.1", "Parus_major.Parus_major1.1", "Pavo_cristatus.AIIM_Pcri_1.0", "Pelodiscus_sinensis" +
                        ".PelSin_1.0", "Pelusios_castaneus.Pelusios_castaneus-1" + ".0", "Periophthalmus_magnuspinnatus.PM.fa", "Peromyscus_maniculatus_bairdii.HU_Pman_2.1",
                        "Petromyzon_marinus.Pmarinus_7.0", "Phascolarctos_cinereus" + ".phaCin_unsw_v4.1", "Phasianus_colchicus.ASM414374v1", "Physeter_catodon.ASM283717v2",
                        "Poecilia_formosa.PoeFor_5.1.2", "Poecilia_latipinna" + ".P_latipinna-1.0", "Poecilia_mexicana.P_mexicana-1.0", "Poecilia_reticulata" + ".Guppy_female_1" +
                        ".0_MT", "Pongo_abelii.Susie_PABv2", "Procavia_capensis" + ".proCap1", "Propithecus_coquereli.Pcoq_1.0", "Pseudonaja_textilis" + ".EBS10Xv2-PRI",
                        "Pteropus_vampyrus.pteVam1", "Pundamilia_nyererei.PunNye1.0", "Pygocentrus_nattereri.Pygocentrus_nattereri-1.0.2", "Rattus_norvegicus.mRatBN7.2",
                        "Rattus_norvegicus_shrspbbbutx" + ".UTH_Rnor_SHRSP_BbbUtx_1.0", "Rattus_norvegicus_shrutx.UTH_Rnor_SHR_Utx", "Rattus_norvegicus_wkybbb.UTH_Rnor_WKY_Bbb_1" +
                        ".0", "Rhinolophus_ferrumequinum" + ".mRhiFer1_v1.p", "Rhinopithecus_bieti.ASM169854v1", "Rhinopithecus_roxellana.Rrox_v1", "Saccharomyces_cerevisiae" +
                        ".R64-1-1", "Saimiri_boliviensis_boliviensis.SaiBol1.0", "Salmo_salar.Ssal_v3" + ".1", "Salmo_trutta.fSalTru1.1", "Sander_lucioperca.SLUC_FBN_1",
                        "Sarcophilus_harrisii.mSarHar1.11", "Scleropages_formosus" + ".fSclFor1.1", "Scophthalmus_maximus.ASM1334776v1", "Serinus_canaria.SCA1",
                        "Seriola_dumerili.Sdu_1.0", "Seriola_lalandi_dorsalis.Sedor1", "Sinocyclocheilus_anshuiensis.SAMN03320099.WGS_v1.1", "Sinocyclocheilus_grahami" +
                        ".SAMN03320097.WGS_v1.1", "Sinocyclocheilus_rhinocerous" + ".SAMN03320098_v1.1", "Sorex_araneus.COMMON_SHREW1", "Sparus_aurata.fSpaAur1.1",
                        "Sphenodon_punctatus.ASM311381v1", "Stegastes_partitus" + ".Stegastes_partitus-1.0.2", "Struthio_camelus_australis.ASM69896v1", "Sus_scrofa.Sscrofa11.1",
                        "Sus_scrofa_bamei.Bamei_pig_v1", "Sus_scrofa_berkshire" + ".Berkshire_pig_v1", "Sus_scrofa_hampshire.Hampshire_pig_v1", "Sus_scrofa_jinhua.Jinhua_pig_v1"
                        , "Sus_scrofa_landrace.Landrace_pig_v1", "Sus_scrofa_largewhite.Large_White_v1", "Sus_scrofa_meishan.Meishan_pig_v1", "Sus_scrofa_pietrain" +
                        ".Pietrain_pig_v1", "Sus_scrofa_rongchang" + ".Rongchang_pig_v1", "Sus_scrofa_tibetan.Tibetan_Pig_v2", "Sus_scrofa_usmarc" + ".USMARCv1.0",
                        "Sus_scrofa_wuzhishan.minipig_v1.0", "Taeniopygia_guttata" + ".bTaeGut1_v1.p", "Takifugu_rubripes.fTakRub1.2", "Tetraodon_nigroviridis.TETRAODON8",
                        "Theropithecus_gelada.Tgel_1.0", "Tupaia_belangeri.TREESHREW", "Tursiops_truncatus.turTru1", "Ursus_americanus.ASM334442v1", "Ursus_maritimus.UrsMar_1.0"
                        , "Varanus_komodoensis.ASM479886v1", "Vicugna_pacos.vicPac1", "Vombatus_ursinus.bare-nosed_wombat_genome_assembly", "Vulpes_vulpes.VulVul2.2",
                        "Xenopus_tropicalis.UCB_Xtro_10.0", "Xiphophorus_couchianus" + ".Xiphophorus_couchianus-4.0.1", "Xiphophorus_maculatus.X_maculatus-5.0-male",
                        "Zalophus_californianus.mZalCal1" + ".pri", "Zonotrichia_albicollis" + ".Zonotrichia_albicollis-1.0.1"};
            }
            case EU -> {
                this.databaseName = "EU";
                this.tableNames = new String[]{"01.01.2023-31.12.2023StatisticsReportMonthlyCfTsAwarded", "13a9ff77-forestfire-cp-apd-kritis-2003-2009_0", "55gy-3b6m",
                        "638_trafikkstasjon-eksport", "cheltuieli-in-spitale-2020-2021", "City_street_Tranche_CP", "contagem-dos-dias-de-ausencia-ao-trabalho-segundo-o-motivo-de"
                        + "-ausencia", "contenedores", "Course details 2021 2022-1", "estat_migr_asyunaa_en", "estat_nrg_ti_coifpm_en", "estat_sts_cobp_q_en", "groupes-active",
                        "HC01", "iga0003", "Lessons 2021 2022-2", "ods040", "OGD_konjunkturmonitor_KonMon_1", "plan_proracuna (1)", "plan_proracuna", "qt3c-wiyg",
                        "rad20200915min", "rad202009tage", "raforkafra1915", "rail-traffic-information", "Rights_Of_Way", "Sacrificare", "Seabird2000", "skoldata",
                        "sldb2021_vyjizdka_vsichni_frekvence_pohlavi", "srednie_wyniki_egzaminu_maturalnego", "tacke-telekomunikacione-mreze", "toegankelijkheidtoiletten",
                        "trafiktaelling", "vidomosti-pro-spravi-pro-bankrutstvo", "vwlusyokuzeni", "zibens_arhiva_dati"};
            }
            case T2D -> {
                this.databaseName = "T2D";
                this.tableNames = new String[]{"0", "1", "10", "100", "101", "102", "103", "104", "105", "106", "107", "108", "109", "11", "110", "111", "112", "113", "114",
                        "115", "116", "117", "118", "119", "12", "120", "121", "122", "123", "124", "125", "126", "127", "128", "129", "13", "130", "131", "132", "133", "134",
                        "135", "136", "138", "139", "14", "140", "141", "142", "143", "145", "146", "147", "148", "15", "150", "152", "153", "154", "155", "156", "157", "158",
                        "159", "16", "160", "161", "162", "164", "165", "166", "168", "169", "17", "170", "171", "172", "173", "174", "175", "177", "179", "18", "180", "182",
                        "183", "185", "186", "187", "188", "189", "19", "190", "191", "192", "193", "195", "196", "197", "198", "199", "2", "20", "200", "201", "202", "203",
                        "204", "205", "206", "208", "209", "21", "210", "211", "212", "213", "214", "215", "216", "217", "218", "219", "22", "220", "221", "222", "223", "224",
                        "225", "226", "227", "228", "23", "230", "231", "232", "234", "235", "236", "237", "238", "239", "24", "240", "241", "242", "243", "244", "245", "246",
                        "247", "248", "249", "25", "250", "251", "252", "253", "254", "257", "258", "259", "26", "260", "261", "262", "264", "266", "267", "268", "269", "27",
                        "270", "271", "272", "274", "275", "276", "278", "279", "28", "280", "281", "282", "283", "284", "285", "286", "287", "29", "290", "291", "293", "294",
                        "295", "296", "297", "298", "3", "30", "300", "303", "305", "307", "308", "309", "31", "311", "312", "313", "314", "315", "316", "317", "318", "319", "32"
                        , "320", "321", "323", "324", "325", "326", "327", "328", "329", "33", "330", "331", "332", "333", "334", "335", "336", "337", "338", "339", "34", "340",
                        "341", "342", "343", "344", "345", "346", "347", "348", "349", "35", "350", "351", "352", "353", "354", "355", "356", "357", "358", "359", "36", "360",
                        "361", "362", "363", "364", "365", "366", "367", "368", "369", "37", "370", "371", "372", "373", "374", "375", "376", "377", "378", "379", "38", "380",
                        "381", "382", "383", "384", "385", "386", "387", "388", "389", "39", "390", "391", "392", "393", "394", "395", "396", "397", "398", "399", "4", "40",
                        "400", "401", "402", "403", "404", "405", "406", "407", "408", "409", "41", "410", "411", "412", "413", "414", "415", "416", "417", "418", "419", "42",
                        "420", "421", "422", "423", "424", "425", "426", "427", "428", "429", "43", "430", "431", "432", "433", "434", "435", "436", "437", "438", "439", "44",
                        "440", "441", "442", "443", "444", "445", "446", "447", "448", "449", "45", "450", "451", "452", "453", "454", "455", "456", "457", "458", "459", "46",
                        "460", "461", "462", "463", "464", "465", "466", "467", "468", "469", "47", "470", "471", "472", "473", "474", "475", "476", "477", "478", "479", "48",
                        "480", "481", "482", "483", "484", "485", "486", "487", "488", "489", "49", "490", "491", "492", "493", "494", "495", "496", "497", "498", "499", "5",
                        "50", "500", "501", "502", "503", "504", "505", "506", "507", "508", "509", "51", "510", "511", "512", "513", "514", "515", "516", "517", "518", "519",
                        "52", "520", "521", "522", "523", "524", "525", "526", "527", "528", "529", "53", "530", "531", "532", "533", "534", "535", "536", "537", "538", "539",
                        "54", "540", "541", "543", "544", "545", "549", "551", "555", "556", "559", "563", "564", "565", "566", "568", "57", "570", "573", "575", "576", "577",
                        "579", "58", "581", "582", "583", "584", "586", "587", "588", "589", "59", "590", "591", "593", "594", "595", "6", "60", "600", "601", "602", "603", "604"
                        , "606", "608", "609", "61", "610", "611", "613", "615", "618", "619", "62", "620", "621", "622", "623", "624", "625", "626", "627", "628", "629", "63",
                        "630", "631", "632", "633", "634", "635", "636", "637", "638", "639", "64", "640", "641", "643", "645", "647", "65", "650", "652", "653", "654", "655",
                        "656", "657", "658", "66", "660", "662", "663", "665", "666", "67", "670", "671", "674", "675", "676", "679", "68", "680", "682", "683", "686", "687",
                        "688", "689", "690", "691", "692", "693", "694", "695", "697", "699", "7", "70", "700", "702", "704", "705", "706", "707", "708", "709", "71", "711",
                        "712", "713", "714", "715", "717", "718", "719", "72", "720", "722", "723", "724", "725", "726", "728", "729", "73", "731", "733", "734", "735", "736",
                        "737", "738", "739", "74", "740", "743", "744", "745", "746", "748", "749", "75", "750", "751", "752", "753", "76", "761", "762", "763", "765", "766",
                        "767", "768", "769", "77", "771", "772", "773", "774", "775", "776", "777", "778", "79", "8", "80", "82", "83", "84", "85", "86", "88", "89", "9", "90",
                        "91", "92", "93", "94", "95", "97", "98", "99"};
            }
            case TESMA -> {
                this.databaseName = "Tesma";
                this.tableNames = new String[]{"main", "extern_info"};
            }
            case WEB_TABLES -> {
                this.databaseName = "WebTables";
                this.tableNames = new String[5000];
                for (int i = 0; i < 5000; i++) {
                    tableNames[i] = String.valueOf(i);
                }
            }
            default -> {
            }
        }
    }

    public enum Dataset {
        TPCH_1, TPCH_10, ANIMAL_CROSSING, DATA_GOV, EU, MUSICBRAINZ, CARS, ENSEMBL_UNIPROT, POPULATION, T2D, TESMA, WEB_TABLES
    }

    public enum NullHandling {
        SUBSET, FOREIGN, EQUALITY, INEQUALITY
    }

    public enum DuplicateHandling {
        AWARE, UNAWARE
    }
}