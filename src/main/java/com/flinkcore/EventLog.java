package com.flinkcore;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author:xmzhou
 * @Date: 2022/7/29 16:23
 * @Description:
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventLog {
    private long guid;
    private String eventId;
    private long timeStamp;
    private String pageId;
    // private int actTimelong;  // 行为时长
}
