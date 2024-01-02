package kopo.poly.service;

import kopo.poly.dto.HadoopDTO;

import java.util.List;

public interface ILocalGzFileReadService {

    List<String> readLocalGzFileCnt(HadoopDTO pDTO) throws Exception;

    List<String> readLocalGzFileIP(HadoopDTO pDTO) throws Exception;

}
