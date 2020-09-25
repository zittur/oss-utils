package com.zittur.oss.utils;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.zittur.oss.conf.GlobalConf;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 检查数据库记录中的文件列表与 OSS 上实际存储的文件之间的差别
 * 功能：
 * 1、查找数据库中存在的但 OSS 上不存在的文件
 * 2、查找 OSS 上存在但数据库中不存在的文件
 */
public class FileCheckUtil {

    static GlobalConf conf = GlobalConf.getInstance();

    private static final Logger logger = Logger.getLogger(FileCheckUtil.class);

    final static String FILEPATH = "filepath";
    final static String LOADERINFO = "loaderinfo";

    static String forderPrefix = conf.getOssPrefix() + "/";

    public static void main(String[] args) throws IOException {

        // 取出数据库中所有的文件路径
        List<String> databaseFilesList = getDatabaseFilesList(conf.getTansatFilePath(), FILEPATH);
        databaseFilesList.addAll(getDatabaseFilesList(conf.getTansatLoaderInfo(), LOADERINFO));
        databaseFilesList.addAll(getDatabaseFilesList(conf.getEcologyFilePath(), FILEPATH));
        databaseFilesList.addAll(getDatabaseFilesList(conf.getEcologyLoaderInfo(), LOADERINFO));
        logger.info("数据库文件总个数: " + databaseFilesList.size());
        // 取出 OSS 下文件夹中的所有文件的文件路径
        List<String> ossFilesList = getOssFilesList((List<String>) conf.getOSSDataFolders().get("folders").unwrapped());
        // 获取数据库中有记录但是 OSS 中没有的文件
        List<String> ossMissingFiles = compareTwoList(databaseFilesList, ossFilesList);
        logger.info("OSS 缺失文件数量: " + ossMissingFiles.size());
//        ossMissingFiles.forEach(logger::info);
        ossMissingFiles.forEach(System.out::println);
        // 获取 OSS 中有但是数据库中没有记录的文件
        List<String> ossRedundantFiles = compareTwoList(ossFilesList, databaseFilesList);
        logger.info("OSS 多余文件数量: " + ossRedundantFiles.size());
//        ossRedundantFiles.forEach(logger::info);
        ossRedundantFiles.forEach(System.out::println);
        // TODO:
        final OSS ossClient = new OSSClientBuilder().build(conf.getOssEndpoint(), conf.getOssAccessKey(), conf.getOssSecretKey());
//        ossClient.deleteObject(conf.getOssBucket(),"workspace/ADMIN/userData/2014/2014-01/中国-东盟植被生长季长度2013_1km_lzw.tif.uploadFile_record");
        deleteFilesFromOss(ossClient,ossRedundantFiles);
        ossClient.shutdown();
    }

    /**
     * @param queryStatement SQL 语句
     * @param fieldName      字段名称
     * @return @link {@link List<String>} 返回指定字段的结果列表
     * @throws
     * @Description 根据 SQL 语句查询数据库，返回指定字段 fieldName 的列表
     * @author JohnZittur
     * @date 2020/9/24 17:06
     */
    private static List<String> getDatabaseFilesList(String queryStatement, String fieldName) {
        logger.info("获取数据库" + fieldName + "字段信息：" + queryStatement);
        List<String> databaseFilesList = new ArrayList<>();
        try {
            Connection connection = null;
            Statement statement = null;
            Class.forName(conf.getDriver());
            // 创建 JDBC 连接
            connection = DriverManager.getConnection(conf.getDatabase(), conf.getUser(), conf.getPassword());
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            ResultSet resultSet = null;
            // 查询数据库
            resultSet = statement.executeQuery(queryStatement);
            while (resultSet.next()) {
                String resultSetString = resultSet.getString(fieldName);
                if (resultSetString != null && resultSetString.startsWith("/")) {
                    databaseFilesList.add(resultSetString);
                }
            }
            // 查询结束关闭连接
            resultSet.close();
            statement.close();
            connection.close();
        } catch (ClassNotFoundException | SQLException e) {
            logger.error(e.getMessage());
        }
        return databaseFilesList;
    }

    /**
     * @param folderKeys 文件夹名称列表
     * @return @link {@link List<String>}
     * @throws
     * @Description 该方法用于查询 OSS 中给定文件夹下的文件列表
     * @author JohnZittur
     * @date 2020/9/24 16:32
     * @Example 例如 OSS workdir 下存在 test1, test2, test3 三个文件夹
     * test1下文件:    a.txt
     * test2下文件:    b1.txt
     *                b2.txt
     * test3下文件:    无文件
     * <p>
     * List<String> folderKeys = Arrays.asList("test1","test2","test3");
     * List<String> ossFilesList = getOssFilesList(folderKeys);
     * <p>
     * 返回 ossFilesList:
     * {“/test1/a.txt”,"/test2/b1.txt","/test2/b2.txt"}
     */
    private static List<String> getOssFilesList(List<String> folderKeys) throws IOException {
        logger.info("获取 OSS 信息...");
        // 创建 OSS 连接客户端
        final OSS ossClient = new OSSClientBuilder().build(conf.getOssEndpoint(), conf.getOssAccessKey(), conf.getOssSecretKey());
        List<String> fileKeys = new ArrayList<>();
        for (String dataFolder : folderKeys) {
            // 添加分隔符 "/"
            String folderKey = forderPrefix + dataFolder + "/";
            // 计算指定文件夹下的文件数量
            final int filesNum = calculateFolderLength(ossClient, conf.getOssBucket(), folderKey);
            fileKeys.addAll(listOssFileKeysByPrefix(ossClient, folderKey, filesNum));
        }
        logger.info("OSS文件总个数: " + fileKeys.size());
        // 关闭 OSS 连接
        ossClient.shutdown();
        return fileKeys;
    }

    /**
     * @param ossClient  OSS 客户端
     * @param bucketName 桶名
     * @param folderKey  文件夹
     * @return @link {@link int}
     * @throws
     * @Description 该方法用来获取 OSS 下某个文件夹的文件总个数
     * @author JohnZittur
     * @date 2020/9/24 19:13
     */
    private static int calculateFolderLength(OSS ossClient, String bucketName, String folderKey) {
        int count = 0;
        ObjectListing objectListing = null;
        do {
            // MaxKey 默认值为 100，最大值为 1000。
            // 创建请求对象
            ListObjectsRequest request = new ListObjectsRequest(bucketName).withPrefix(folderKey).withMaxKeys(1000);
            if (objectListing != null) {
                request.setMarker(objectListing.getNextMarker());
            }
            // 发送请求
            objectListing = ossClient.listObjects(request);
            List<OSSObjectSummary> sums = objectListing.getObjectSummaries();
            for (OSSObjectSummary summary : sums) {
                // 过滤结果中的文件夹，只保留文件
                if (!summary.getKey().endsWith("/")) {
                    count += 1;
                }
            }
        } while (objectListing.isTruncated());
        logger.info("文件夹 " + folderKey + "下文件总数为： " + count);
        return count;
    }

    /**
     * @param ossClient OSS 客户端
     * @param folderKey 文件夹
     * @param filesNum  该文件夹下的文件数量
     * @return @link {@link List<String>}
     * @throws
     * @Description 返回指定文件夹下的所有文件 folderKey like "${workdir}/cog-tif/";
     * @author JohnZittur
     * @date 2020/9/24 17:08
     */
    private static List<String> listOssFileKeysByPrefix(OSS ossClient, String folderKey, int filesNum) throws IOException {
        String nextMarker = null;
        // 构造ListObjectsRequest请求，指定List大小。
        List<String> filesKeys = new ArrayList<>(filesNum);
        final int maxKeys = 200;
        ObjectListing objectListing;
        // 遍历所有文件。
        do {
            objectListing = ossClient.listObjects(new ListObjectsRequest(conf.getOssBucket())
                    .withMarker(nextMarker).withMaxKeys(maxKeys).withPrefix(folderKey));
            List<OSSObjectSummary> sums = objectListing.getObjectSummaries();
            for (OSSObjectSummary s : sums) {
                if (!s.getKey().endsWith("/")) {
                    //将s前面的workdir去掉，保持和数据库中的一致
                    filesKeys.add(s.getKey().replace("workspace/ADMIN/userData/", "/"));
                }
            }
            nextMarker = objectListing.getNextMarker();
        } while (objectListing.isTruncated());
        return filesKeys;
    }

    /**
     * @param sourceList
     * @param targetList
     * @return @link {@link List<String>}
     * @throws
     * @Description 找到两个 List：sourceList 和 targetList 中不同的文件，返回 sourceList 中有，但是 targetList 没有的文件列表
     * 情况1： 当 sourceList 是数据库的文件列表，说明结果是数据库中存在，但是 OSS 中不存在，需要上传缺失文件
     * 情况2： 当 sourceList 是 OSS 的文件列表，说明结果是 OSS 中存在，但是数据库中不存在对应记录，需要在 OSS 中删除这些无用数据
     * 原则：维护数据库，以数据库记录为准
     * @author JohnZittur
     * @date 2020/9/24 16:53
     */
    private static List<String> compareTwoList(List<String> sourceList, List<String> targetList) {

        List<String> diffList = new ArrayList<>();
        // 将 targetList 中的元素放入 HashMap
        Map<String, Integer> map = new HashMap<>(targetList.size());
        for (String targetString : targetList) {
            map.put(targetString, 1);
        }
        // 寻找 sourceList 和 targetList 两者不同的元素
        for (String sourceString : sourceList) {
            if (map.get(sourceString) == null) {
                diffList.add(sourceString);
            }
        }
        return diffList;
    }

    /**
     *
     * @Description 根据 ossDeleteFiles 列表，在 OSS 上删除对应文件
     * @param ossClient
     * @param ossDeleteFiles
     * @return @link
     * @throws
     * @author JohnZittur
     * @date 2020/9/24 22:25
     */
    private static void deleteFilesFromOss(OSS ossClient, List<String> ossDeleteFiles) {
        ossDeleteFiles.forEach(s -> {
            String ossDeleteFile = conf.getOssPrefix() + s;
            ossClient.deleteObject(conf.getOssBucket(), ossDeleteFile);
        });
    }

}
