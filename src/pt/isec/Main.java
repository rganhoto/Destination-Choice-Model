package pt.isec;

import pt.isec.dbpr.WP_dbPediaCompare;
import pt.isec.dbpr.WP_dbPediaDownloader;
import pt.isec.fc.FC_ConfigData;
import pt.isec.fc.FC_ExportCityCluster;
import pt.isec.fc.FC_ExportShapeFile;
import pt.isec.fc.FC_GenerateCluster;
import pt.isec.fd.FD_ConfigData;
import pt.isec.fd.FD_FactualReader;
import pt.isec.msh.LogWriter;
import pt.isec.msh.MSClient;

public class Main {

    public static void main(String[] args) {
        MSClient mscli = new MSClient();
        try {

            //FC_ConfigData.RunRubberCLusters();

            //INICIALIZA CONFIGURAÇÕES DA CONFIG
            ConfigReader.ReadConfigFromFile();

            //INICIALIZA CONFIGURAÇÕES DO SQL
            MSClient.mysql_address = ConfigReader.mysql_address;
            MSClient.mysql_port = ConfigReader.mysql_port;
            MSClient.mysql_database = ConfigReader.mysql_database;
            MSClient.mysql_user = ConfigReader.mysql_user;
            MSClient.mysql_password = ConfigReader.mysql_password;
            MSClient.table_prefix = ConfigReader.mysql_table_prefix;

            //INICIALIZA CONFIGURAÇÕES DO FACTUAL
            FD_ConfigData.Initialize(
                    ConfigReader.factual_key,
                    ConfigReader.factual_secret,
                    ConfigReader.factual_locality,
                    ConfigReader.factual_region,
                    ConfigReader.factual_country);
            mscli.OpenConnection();


            FC_ConfigData.Initialize(
                    ConfigReader.cc_epsilon_type,
                    Integer.valueOf(ConfigReader.cc_zoom1_minpoints),
                    Float.valueOf(ConfigReader.cc_zoom1_epsilon),
                    Integer.valueOf(ConfigReader.cc_zoom2_minpoints),
                    Float.valueOf(ConfigReader.cc_zoom2_epsilon),
                    Integer.valueOf(ConfigReader.cc_zoom3_minpoints),
                    Float.valueOf(ConfigReader.cc_zoom3_epsilon));

            WP_dbPediaDownloader.dbpedia_word = ConfigReader.dbpedia_word;
            WP_dbPediaDownloader.path = ConfigReader.dbpedia_path;
            WP_dbPediaDownloader.file1 = ConfigReader.dbpedia_file1;
            WP_dbPediaDownloader.file2 = ConfigReader.dbpedia_file2;
            WP_dbPediaDownloader.file3 = ConfigReader.dbpedia_file3;
            WP_dbPediaDownloader.file4 = ConfigReader.dbpedia_file4;
            WP_dbPediaDownloader.file5 = ConfigReader.dbpedia_file5;
            WP_dbPediaDownloader.file6 = ConfigReader.dbpedia_file6;
            WP_dbPediaDownloader.file7 = ConfigReader.dbpedia_file7;
            WP_dbPediaDownloader.file8 = ConfigReader.dbpedia_file8;
            WP_dbPediaDownloader.file9 = ConfigReader.dbpedia_file9;
            WP_dbPediaDownloader.file10 = ConfigReader.dbpedia_file10;


            //FC_ExportCityCluster.ExportCategoryClusterDataFile(3,mscli);

            //CRIAR BD's NECESSÁRIAS
            FD_FactualReader.CreateDatabase(mscli);
            FC_GenerateCluster.CreateTables(mscli);
            WP_dbPediaDownloader.CreateDataBase(mscli);
            //FC_ExportCityCluster.ExportCategoryClusterDataFiles(mscli);
            //   FD_FactualReader.ReadFactualCategories(mscli);
            //   FD_FactualReader.ReadData(mscli);
            //   FC_ExportCityCluster.ExportSimpleClusterDataFiles(mscli);
            //FC_ExportCityCluster.ExportSimpleClusterDataFiles(mscli);

            //FC_ExportCityCluster.ExportSimpleClusterDataFiles(mscli);

            //FC_GenerateCluster.CreateCategoryClusters(mscli);
            //FD_FactualReader.ReadFactualCrossWalk(mscli);
            //WP_dbPediaDownloader.DownloadDataFromDBPedia(mscli);
            //WP_dbPediaDownloader.GetDbpediaData_TEST(mscli);
            //WP_dbPediaDownloader.FillDbPediaTable(mscli,false);
            //WP_dbPediaCompare.CompareFactualVSdbPedia(mscli);
            //WP_dbPediaCompare.ComparePreInsertedData(mscli);
            //WP_dbPediaCompare.CompareWebSite_TEST(mscli);
            //FD_FactualReader.ReadFactualCategories(mscli);

            //RUBBER CLUSERS

            //FC_GenerateCluster.GetNearestNeighbor(mscli, 10);




            boolean ReadFactualData = false;
            boolean GetCrosswalkData = false;
            boolean CreateSimpleClusters = false;
            boolean CreateCategoryClusters = false;

            boolean ExportSimpleClusterData = false;
            boolean ExportCategoryClusterData = false;

            boolean DownloadDbPedia = false;
            boolean ConvertDbPediaData = false;
            boolean CreateSampleARFF = false;
            boolean CreateTestArff = false;
            boolean GetNearestNEighbor = false;
            //boolean GetNearPoints = false;

            int ArffLineNumber = 0;




            if (args.length == 0) {
                System.out.println("Please use the application with the next arguments:");
                System.out.println("-d : Download Factual Data");
                System.out.println("-d[startname] : Continue Data from [startname]");
                System.out.println("-cw : CrossWalk");
                System.out.println("-cw[factual_id] : StartFromFactualID");
                System.out.println();
                //System.out.println("-n : Find nearest neighbor");
                System.out.println("-n[N] : Find nearest N neighbor");
                System.out.println("Global Clusters:");
                System.out.println("-cs : Create Clusters");
                System.out.println("-es : Export the created Clusters");
                System.out.println();
                System.out.println("Clusters By category:");
                System.out.println("-cc : Create Clusters");
                System.out.println("-ec : Export the created Clusters");
                System.out.println();

                System.out.println("-pd : Download DBPedia Data");
                System.out.println("-pc : Convert Raw Data to Structured Table");
                System.out.println("-ps : Create Sample ARFF");
                System.out.println("-pt[number] : Create Test Data ARFF");


                System.out.println();
                System.out.println();
                return;
            }

            boolean ContinueFactualData = false;
            String FactualDataToContinue = "";
            String CrossWalkDataToContinue = "";
            int NearPointEpsilon = 0;

            for (String arg : args) {
                if (arg.equals("-d"))
                    ReadFactualData = true;
                else if (arg.startsWith("-d")) {
                    FactualDataToContinue = arg.substring(2);
                    ContinueFactualData = true;
                    System.out.println("Continue Data " + FactualDataToContinue);
                }

                if (arg.startsWith("-n")) {
                    NearPointEpsilon = Integer.valueOf(arg.substring(2));
                    GetNearestNEighbor = true;
                }


                if (arg.equals("-cs"))
                    CreateSimpleClusters = true;

                if (arg.equals("-es"))
                    ExportSimpleClusterData = true;

                if (arg.equals("-cc"))
                    CreateCategoryClusters = true;

                if (arg.equals("-ec"))
                    ExportCategoryClusterData = true;

                if (arg.equals("-cw"))
                    GetCrosswalkData = true;
                else if (arg.startsWith("-cw")) {
                    CrossWalkDataToContinue = arg.substring(3);
                    GetCrosswalkData = true;
                }

                if (arg.equals("-pd"))
                    DownloadDbPedia = true;

                if (arg.equals("-pc"))
                    ConvertDbPediaData = true;

                if (arg.equals("-ps"))
                    CreateSampleARFF = true;

                if (arg.startsWith("-pt")) {
                    ArffLineNumber = Integer.parseInt(arg.substring(3));
                    CreateTestArff = true;
                }

                if(arg.equals("-rc"))
                {
                    String _args[] = new String[args.length-1];
                    for(int i=1;i<args.length;i++)
                    {
                        _args[i-1] = args[i];
                    }

                    FC_ConfigData.RunRubberCLusters2(_args);
                }

            }


            //LER DADOS DO FACTUAL

            long tStart = System.currentTimeMillis();

            if (ReadFactualData) {
                //FD_FactualReader.ReadFactualCategories(mscli);
                FD_FactualReader.ReadData(mscli);
            }

            if (ContinueFactualData)
                FD_FactualReader.ContinueRead(mscli, FactualDataToContinue);

            if (GetNearestNEighbor)
                FC_GenerateCluster.GetNearestNeighbor(mscli, NearPointEpsilon);

/*            if(GetNearPoints)
                FC_GenerateCluster.NumberPointsNearNeighbor(mscli,NearPointEpsilon);
*/
            if (CreateSimpleClusters) {
                FC_GenerateCluster.CreateSimpleClusters(mscli);
            }

            if (CreateCategoryClusters) {
                FC_GenerateCluster.CreateCategoryClusters(mscli);
            }

            if (ExportSimpleClusterData) {
                FC_ExportCityCluster.ExportSimpleClusterDataFiles(mscli);
            }

            if (ExportCategoryClusterData) {
                FC_ExportCityCluster.ExportCategoryClusterDataFiles(mscli);
            }
//            FC_ExportCityCluster.ExportCategoryClusterDataFiles(mscli);

            if (GetCrosswalkData) {
                FD_FactualReader.ReadFactualCrossWalk(mscli, CrossWalkDataToContinue);
            }

            //RubberClustersControl.CreateShapeFiles(mscli.table_prefix);

            if (DownloadDbPedia)
                WP_dbPediaDownloader.DownloadDataFromDBPedia(mscli);

            if (ConvertDbPediaData)
                WP_dbPediaDownloader.FillDbPediaTable(mscli, true);


            if (CreateSampleARFF) {
                WP_dbPediaCompare.CompareFactualVSdbPedia(mscli);
            }

            if (CreateTestArff) {

            }


            System.out.println("Program END.");


            long tEnd = System.currentTimeMillis();
            long tDelta = tEnd - tStart;
            double elapsedSeconds = tDelta / 1000.0;
            //SendMail("ganhoto@live.com","Teste Email","Este é um teste de um email enviado por java");
            MailSender.SendMail(ConfigReader.email, "MIS SUCESSO", "Processo termimado com sucesso Tempo(s):" + Double.toString(elapsedSeconds));

        } catch (Exception e) {
            e.printStackTrace();
            LogWriter.WriteErrorLogs(e);
            MailSender.SendMail(ConfigReader.email, "MIS ERRO", e.getMessage());
        } finally {
            mscli.CloseConnectionIfOpen();

        }
    }
}
