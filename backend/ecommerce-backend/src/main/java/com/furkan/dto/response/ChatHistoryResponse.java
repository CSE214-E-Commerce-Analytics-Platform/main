package com.furkan.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ChatHistoryResponse {
    private UUID id;
    private String title;
    private String initialQuery;
    private Long userId;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
