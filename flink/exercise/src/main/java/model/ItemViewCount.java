package model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ItemViewCount {
    private String itemId;
    private long windowEnd;
    private long viewCount;

    public String toString() {
        return "itemId=" + this.itemId + ",windowEnd=" + windowEnd + ",viewCount=" + viewCount;
    }
}
