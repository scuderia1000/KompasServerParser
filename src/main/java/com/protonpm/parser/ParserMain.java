package com.protonpm.parser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

/**
 * Created by Ershov-V-V on 26.09.2016.
 */
public class ParserMain {
    private static final String url = "jdbc:mysql://localhost:3306/kompas?autoReconnect=true&useSSL=false";
    private static final String user = "root";
    private static final String pass = "root";
    private static final String parsedURL = "http://168.192.10.181:1947/ru.7.0.alp/tab_sessions.html?haspid=0&featureid=-1&vendorid=0&productid=0&filterfrom=1&filterto=100?";
    private static Connection con;
    private static Statement stmt;
//    private static ResultSet rs;

    public static void main(String[] args) {
//        String querySelect = "SELECT * FROM users";
        String queryInsertStart = "INSERT INTO `kompas`.`users` " +
                "(`haspid`, `prid`, `productname`, `fid`, `fn`, `cli`, `usr`, `mch`, `lt`) " +
                "VALUES (";
        try {
            URL serverHost = new URL(parsedURL);
            con = DriverManager.getConnection(url, user, pass);
            stmt = con.createStatement();
            StringBuilder sb = new StringBuilder();
            String s = "";
            int colDuplicatedEntry = 0, colAddedEntry = 0;
            Date date = new Date(System.currentTimeMillis());
            System.out.println(date);
//            while (System.currentTimeMillis() < date.getTime() + 3600000) {
                BufferedReader inputStream = new BufferedReader(new InputStreamReader(serverHost.openStream(), "UTF-8"));
                while ((s = inputStream.readLine()) != null) {

                    if (s.contains("\"haspid\"")) {
                        sb.append("'");
                        sb.append(s.substring(s.indexOf(":\"") + 2, s.length() - 2));
                        sb.append("', '");
                    }
                    if (s.contains("\"prid\"")) {
                        sb.append(s.substring(s.indexOf(":\"") + 2, s.length() - 2));
                        sb.append("', '");
                    }
                    if (s.contains("\"productname\"")) {
                        sb.append(s.substring(s.indexOf(":\"") + 2, s.length() - 2));
                        sb.append("', '");
                    }
                    if (s.contains("\"fid\"")) {
                        sb.append(s.substring(s.indexOf(":\"") + 2, s.length() - 2));
                        sb.append("', '");
                    }
                    if (s.contains("\"fn\"")) {
                        sb.append(s.substring(s.indexOf(":\"") + 2, s.length() - 2));
                        sb.append("', '");
                    }
                    if (s.contains("\"cli\"")) {
                        sb.append(s.substring(s.indexOf(":\"") + 2, s.length() - 2));
                        sb.append("', '");
                    }
                    if (s.contains("\"usr\"")) {
                        sb.append(s.substring(s.indexOf(":\"") + 2, s.length() - 2));
                        sb.append("', '");
                    }
                    if (s.contains("\"mch\"")) {
                        sb.append(s.substring(s.indexOf(":\"") + 2, s.length() - 2));
                        sb.append("', '");
                    }
                    if (s.contains("\"lt\"")) {
//					date = new Date();
//					SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd, HH:mm:ss");
//					sdf.parse(s.substring(s.indexOf(":\"") + 2, s.length() - 2));
//					System.out.println(sdf.format(date));
                        sb.append(s.substring(s.indexOf(":\"") + 2, s.length() - 2));
                        sb.append("');");
                    }
                    if (s.contains("},")) {
//                    fileStream.write(sb + "\r\n");
                        try {
                            stmt.executeUpdate(queryInsertStart + sb.toString());
                            System.out.println(sb.toString());
                            colAddedEntry++;
                        } catch (SQLException e) {
                            colDuplicatedEntry++;
//						e.printStackTrace();
                        }
                        sb.delete(0, sb.length());
                    }
                }
                System.out.println("Added entry = " + colAddedEntry);
                System.out.println("Duplicated entry = " + colDuplicatedEntry);
                sb.delete(0, sb.length());
                s = "";
                colAddedEntry = 0;
                colDuplicatedEntry = 0;
                inputStream.close();
//                try {
//                    Thread.sleep(60000);
//                } catch (InterruptedException e) {}
//            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                con.close();
            } catch (SQLException se) {
            }
            try {
                stmt.close();
            } catch (SQLException se) {
            }
        }


    }

}
