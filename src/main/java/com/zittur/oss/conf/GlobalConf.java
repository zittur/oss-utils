package com.zittur.oss.conf;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;

public class GlobalConf {

    private static Config config = ConfigFactory.load("conf/application.conf");

    private static final GlobalConf instance = new GlobalConf();

    public GlobalConf() {}

    public static GlobalConf getInstance() {return instance;};

    public String getOssBucket() {
        return config.getString("oss.bucket");
    }

    public String getOssEndpoint() {
        return config.getString("oss.endpoint");
    }

    public String getOssPrefix() {
        return config.getString("oss.prefix");
    }

    public String getOssAccessKey() {
        return config.getString("oss.accessKey");
    }

    public String getOssSecretKey() {
        return config.getString("oss.secretKey");
    }

    public String getObsBucket() {
        return config.getString("obs.bucket");
    }

    public String getObsEndpoint() {
        return config.getString("obs.endpoint");
    }

    public String getObsPrefix() {
        return config.getString("obs.prefix");
    }

    public String getObsAccessKey() {
        return config.getString("obs.accessKey");
    }

    public String getObsSecretKey() {
        return config.getString("obs.secretKey");
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

    public String getTansatFilePath()
    {
        return config.getString("sql.tansat.filepath");
    }

    public String getTansatLoaderInfo() {
        return config.getString("sql.tansat.loaderinfo");
    }

    public String getEcologyFilePath() {
        return config.getString("sql.ecology.filepath");
    }

    public String getEcologyLoaderInfo() {
        return config.getString("sql.ecology.loaderinfo");
    }

    public ConfigObject getOSSDataFolders() {
        return config.getObject("data");
    }


}
