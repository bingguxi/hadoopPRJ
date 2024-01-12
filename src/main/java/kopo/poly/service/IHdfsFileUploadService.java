package kopo.poly.service;

import kopo.poly.dto.HadoopDTO;

public interface IHdfsFileUploadService {

    void uploadHdfsFile(HadoopDTO pDTO) throws Exception;

    void uploadHdfsFileContents(HadoopDTO pDTO) throws Exception;

}
