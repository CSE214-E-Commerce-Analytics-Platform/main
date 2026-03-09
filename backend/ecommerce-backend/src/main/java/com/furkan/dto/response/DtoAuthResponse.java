package com.furkan.dto.response;

import com.furkan.dto.BaseDto;
import com.furkan.enums.RoleType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DtoAuthResponse {
    private String accessToken;
    private String refreshToken;
    private RoleType role;
}
