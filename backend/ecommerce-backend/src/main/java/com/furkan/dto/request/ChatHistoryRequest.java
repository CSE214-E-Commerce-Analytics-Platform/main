package com.furkan.dto.request;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ChatHistoryRequest {
    private String title;
    private String initialQuery;
}
