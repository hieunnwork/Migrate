package cmd;

import service.Service;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;

import java.io.FileReader;
import java.io.*;
import java.util.*;

public class Main {
    public static void main(String[] args) {
        long ti, tf;
        Map<String, Integer> tableNameToColumnsMap = new HashMap<>();
        ti = System.currentTimeMillis();
        System.out.println("Start : " + ti);
        try {
            String jsonFilePath = "config/environment.json";

            FileReader reader = new FileReader(jsonFilePath);
            JsonObject jsonData = new Gson().fromJson(reader, JsonObject.class);

            JsonObject dbInformation = jsonData.getAsJsonObject("DB_INFORMATION");
            String sqlServerUrl = dbInformation.get("SQL_SERVER_URL").getAsString();
            String sqlServerUser = dbInformation.get("SQL_SERVER_USER").getAsString();
            String sqlServerPassword = dbInformation.get("SQL_SERVER_PASSWORD").getAsString();
            String PGSServerUrl = dbInformation.get("PGS_SERVER_URL").getAsString();
            String PGSServerUser = dbInformation.get("PGS_SERVER_USER").getAsString();
            String PGSServerPassword = dbInformation.get("PGS_SERVER_PASSWORD").getAsString();

            JsonArray tableInformation = jsonData.getAsJsonArray("TABLE_INFORMATION");
            for (int i = 0; i < tableInformation.size(); i++) {
                JsonObject tableInfo = tableInformation.get(i).getAsJsonObject();
                int numberColumns = tableInfo.get("NumberColumns").getAsInt();
                String tableName = tableInfo.get("TableName").getAsString();
                tableNameToColumnsMap.put(tableName, numberColumns);
            }

            int numThreads = 5;
            List<Thread> threads = new ArrayList<>();
            List<Map.Entry<String, Integer>> entryList = new ArrayList<>(tableNameToColumnsMap.entrySet());
            for (int index = 0; index < entryList.size(); index++) {
                for (int j = 0; j < numThreads; j++) {
                    String tableName = entryList.get(index).getKey();
                    int numberColumns = entryList.get(index).getValue();
                    if (tableName.isEmpty() || numberColumns==0){
                        break;
                    }
                    Thread thread = new Service(sqlServerUrl, sqlServerUser, sqlServerPassword,PGSServerUrl, PGSServerUser, PGSServerPassword, tableName, numberColumns);
                    thread.start();
                    threads.add(thread);
                    index++;
                }
                if (index > entryList.size()) {
                    break;
                }
                for (Thread thread : threads) {
                    try {
                        thread.join();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

/*                for (Thread thread : threads) {
                    if (thread instanceof SqlServerService) {
                        SqlServerService dataReaderThread = (SqlServerService) thread;
                        tableDataMap = dataReaderThread.getTableDataMap();

                    }
                }*/
            }


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        tf = System.currentTimeMillis();
        System.out.println("Finish. Total time: " + (tf - ti));
    }

}
