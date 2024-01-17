package kopo.poly.component.impl;

import kopo.poly.HadoopPrjApplication;
import kopo.poly.component.IHdfsExam;
import kopo.poly.dto.HadoopDTO;
import kopo.poly.service.IHdfsFileDownloadService;
import kopo.poly.service.IHdfsFileReadService;
import kopo.poly.service.IHdfsFileUploadService;
import kopo.poly.service.ILocalGzFileReadService;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.stereotype.Component;

import java.util.List;

@Log4j
@RequiredArgsConstructor
@Component
public class Exam04 implements IHdfsExam {

    // Gz파일 서비스
    private final ILocalGzFileReadService localGzFileReadService;

    // HDFS 파일 업로드 서비스
    private final IHdfsFileUploadService hdfsFileUploadService;

    // HDFS 업로드된 파일 내용보기
    private final IHdfsFileReadService hdfsFileReadService;

    private final IHdfsFileDownloadService hdfsFileDownloadService;

    @Override
    public void doExam() throws Exception {

        log.info("[실습1.] HDFS에 저장된 access_log.gz 파일 다운로드 ");

        HadoopDTO pDTO = new HadoopDTO();

        // 하둡에 저장될 파일 정보
        pDTO.setHadoopUploadPath("/01/02");
        pDTO.setHadoopUploadFileName("access_log.gz");

        // 내 컴퓨터에 존재하는 업로드할 파일 정보
        pDTO.setLocalUploadPath("c:/hadoop_data");
        pDTO.setLocalUploadFileName("hdfs_access_log.gz");

        hdfsFileDownloadService.downloadHdfsFile(pDTO);

        pDTO = null;

        log.info("[실습1.결과] 파일 다운로드 완료 ");

    }
}
