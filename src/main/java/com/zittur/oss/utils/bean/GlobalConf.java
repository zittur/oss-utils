package com.zittur.oss.utils.bean;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class GlobalConf {

    private static Config config = ConfigFactory.load("conf/application.conf");

    private static final GlobalConf instance = new GlobalConf();

    public GlobalConf() {}

    public static GlobalConf getInstance() {return instance;};

    public String getBucket() {
        return config.getString("s3.bucket");
    }

    public String getEndpoint() {
        return config.getString("s3.endpoint");
    }

    public String getWorkdir() {
        return config.getString("s3.workdir");
    }

    public String getAccessKey() {
        return config.getString("s3.accessKey");
    }

    public String getSecretKey() {
        return config.getString("s3.secretKey");
    }

    public String getDriver() {
        return config.getString("db.driver");
    }

    public String getDatabase() {
        return config.getString("db.database");
    }

    public String getUser() {
        return config.getString("db.user");
    }

    public String getPassword() {
        return config.getString("db.password");
    }
}
