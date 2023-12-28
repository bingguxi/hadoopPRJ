package kopo.poly.service;

import kopo.poly.dto.HadoopDTO;

public interface IHdfsFileDownloadService {

    void downloadHdfsFile(HadoopDTO pDTO) throws Exception;

}
