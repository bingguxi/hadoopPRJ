package kopo.poly.service.impl;

import kopo.poly.dto.HadoopDTO;
import kopo.poly.service.AbstractHadoopConf;
import kopo.poly.service.IHdfsFileDownloadService;
import kopo.poly.util.CmmUtil;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Service;
import java.io.IOException;


@Log4j
@Service
public class HdfsFileDownloadService extends AbstractHadoopConf implements IHdfsFileDownloadService {

    @Override
    public void downloadHdfsFile(HadoopDTO pDTO) throws Exception {

        log.info(this.getClass().getName() + "downloadFile Start!");

        FileSystem hdfs = FileSystem.get(this.getHadoopConfiguration());

        // 다운로드 하기 위한 하둡에 저장된 파일의 폴더
        String hadoopUploadFilePath = CmmUtil.nvl(pDTO.getHadoopUploadPath());

        String hadoopUploadFileName = CmmUtil.nvl(pDTO.getHadoopUploadFileName());

        String hadoopFile = hadoopUploadFilePath + "/" + hadoopUploadFileName;

        Path path = new Path(hadoopFile);

        log.info("HDFS Exists : " + hdfs.exists(path));

        if (hdfs.exists(path)) {

            Path localPath = new Path(CmmUtil.nvl(pDTO.getLocalUploadPath()) + "/" + CmmUtil.nvl(pDTO.getLocalUploadFileName()));

            hdfs.copyToLocalFile(path, localPath);

        }
        hdfs.close();

        log.info(this.getClass().getName() + ".downloadFile End!");


    }
}
