package com.furkan.dto.response;

import com.furkan.dto.BaseDto;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class DtoChatHistory extends BaseDto {
    private String title;
    private String initialQuery;
    private Long userId;
}
