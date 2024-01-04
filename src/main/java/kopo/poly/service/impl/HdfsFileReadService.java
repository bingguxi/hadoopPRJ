package kopo.poly.service.impl;

import kopo.poly.dto.HadoopDTO;
import kopo.poly.service.AbstractHadoopConf;
import kopo.poly.service.IHdfsFileReadService;
import kopo.poly.util.CmmUtil;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.stereotype.Service;
import org.apache.hadoop.fs.Path;

@Log4j
@Service
public class HdfsFileReadService extends AbstractHadoopConf implements IHdfsFileReadService {

    @Override
    public String readHdfsFile(HadoopDTO pDTO) throws Exception {

        log.info(this.getClass().getName() + ".readHdfsFile Start!");

        // 하둡 분산 파일 시스템 객체
        FileSystem hdfs = FileSystem.get(this.getHadoopConfiguration());

        String hadoopFile = CmmUtil.nvl(pDTO.getHadoopUploadPath()) + "/" + CmmUtil.nvl(pDTO.getHadoopUploadFileName());

        Path path = new Path(hadoopFile);

        log.info("HDFS Exists : " + hdfs.exists(path));

        String readLog = "";

        if (hdfs.exists(path)) {

            FSDataInputStream inputStream = hdfs.open(path);
            readLog = inputStream.readUTF();
            inputStream.close();

        }

        log.info(this.getClass().getName() + ".readHdfsFile End!");

        return readLog;
    }
}
