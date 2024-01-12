package kopo.poly.component.impl;

import kopo.poly.component.IHdfsExam;
import kopo.poly.dto.HadoopDTO;
import kopo.poly.service.impl.HdfsFileUploadService;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.stereotype.Component;

@Log4j
@RequiredArgsConstructor
@Component
public class Exam01 implements IHdfsExam {

    private final HdfsFileUploadService hdfsFileUploadService;

    @Override
    public void doExam() throws Exception {

        HadoopDTO pDTO = new HadoopDTO();

        // 하둡에 저장될 파일 정보
        pDTO.setHadoopUploadPath("/01/02");
        pDTO.setHadoopUploadFileName("access_log.gz");

        // 내 컴퓨터에 존재하는 업로드할 파일 정보
        pDTO.setLocalUploadPath("c:/hadoop_data");
        pDTO.setLocalUploadFileName("access_log.gz");

        hdfsFileUploadService.uploadHdfsFile(pDTO);

    }
}
