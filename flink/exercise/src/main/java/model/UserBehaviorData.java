package model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserBehaviorData {
    private Long userId;
    private Long itemId;
    private Long categoryId;
    private String behavior;
    private Long timestamp;
}

