package kopo.poly.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class HadoopDTO {

    // 하둡에 업로드 되는 파일 이름
    private String hadoopUploadFileName;

    // 하둡에 업로드 되는 파일 경로
    private String hadoopUploadPath;

    // 하둡에 업로드 할 내 컴퓨터에 존재하는 파일 이름
    private String localUploadFileName;

    //
    private String localUploadPath;
    long lineCnt;
    List<String> contentList;
    String regExp;

}
