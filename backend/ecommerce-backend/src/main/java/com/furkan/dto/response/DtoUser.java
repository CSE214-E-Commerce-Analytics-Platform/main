package com.furkan.dto.response;

import com.furkan.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class DtoUser extends BaseDto {
    private String email;
    private String roleType;
    private boolean isActive;
}
