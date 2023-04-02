package model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserLoginData {
    private String ip;
    private String username;
    private String operateUrl;
    private String time;
    private String loginAttempt;
}
