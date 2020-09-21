package com.zittur.oss.utils.sample;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.zittur.oss.utils.bean.GlobalConf;

import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class TanSatsOssUtils {

    static GlobalConf conf = GlobalConf.getInstance();

    public static void main(String[] args) throws IOException {
        System.out.println(conf.getAccessKey());
        List<String> missingFiles = checkObjectExists(getOSSUrl());
        System.out.println("缺失文件数量:" + missingFiles.size());
        // 打印缺失文件到控制台
        missingFiles.forEach((System.out::println));
        // 保存缺失文件列表到本地文件
        saveResult2Local(missingFiles);
    }

    //step1 : 用 JDBC 连接数据库，查询 fx_tansat 表下的 loaderinfo 字段数据
    private static List<String> getLoaderInfoList() {
        Connection connection = null;
        Statement statement = null;
        List<String> loaderInfoList = new ArrayList<>();
        try {
            Class.forName(conf.getDriver());
            connection = DriverManager.getConnection(conf.getDatabase(), conf.getUser(), conf.getPassword());
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT loaderinfo FROM fx_tansat");
            while (resultSet.next()) {
                String loaderinfo = resultSet.getString("loaderinfo");
                if (loaderinfo != null) {
                    loaderInfoList.add(loaderinfo);
                }
            }
            resultSet.close();
            statement.close();
            connection.close();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return loaderInfoList;
    }

    // step2: 拼接 OSS 路径
    private static List<String> getOSSUrl() {
        final List<String> loaderInfoList = getLoaderInfoList();
        List<String> ossUrl = new ArrayList<>();
        //拼接 Object url
        for (String loaderInfo : loaderInfoList) {
            ossUrl.add(conf.getWorkdir() + loaderInfo);
        }
        return ossUrl;
    }

    // step3: OSSClient 发送请求，判断 OSS 中是否存在该文件
    private static List<String> checkObjectExists(List<String> objectKeys) {
        boolean exist;
        final OSS ossClient = new OSSClientBuilder().build(conf.getEndpoint(), conf.getAccessKey(), conf.getSecretKey());
        List<String> missingFiles = new ArrayList<>();
        for (String objectKey : objectKeys) {
            System.out.println("checking " + objectKey + " ......");
            exist = ossClient.doesObjectExist(conf.getBucket(), objectKey);
            if (!exist) {
                missingFiles.add(objectKey);
            }
        }
        ossClient.shutdown();
        return missingFiles;
    }

    // 保存结果到本地文件
    private static void saveResult2Local(List<String> missingFiles) throws IOException {
        String path = "D:\\missingFiles.txt";
        FileOutputStream fos = new FileOutputStream(path, true);
        for (String s : missingFiles) {
            fos.write((s + "\n").getBytes());
        }
    }
}
