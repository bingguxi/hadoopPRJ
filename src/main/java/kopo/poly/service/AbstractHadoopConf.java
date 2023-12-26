package kopo.poly.service;

import org.apache.hadoop.conf.Configuration;

public abstract class AbstractHadoopConf {

    String namenodeHost = "192.168.175.132";

    String namenodePort = "9000";

    String yarnPort = "8080";

    public Configuration getHadoopConfiguration() {

        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "hdfs://" + namenodeHost + ":" + namenodePort);

        conf.set("yarn.resourcemanager.address", namenodeHost + ":" + yarnPort);

        return conf;
    }
}
