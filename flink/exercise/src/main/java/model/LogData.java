package model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LogData {
    private String ip;
    private String userId;
    private long eventTime;
    private String method;
    private String url;
}
