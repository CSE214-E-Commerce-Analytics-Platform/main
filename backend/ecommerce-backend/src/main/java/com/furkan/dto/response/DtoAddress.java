package com.furkan.dto.response;

import com.furkan.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class DtoAddress extends BaseDto {
    private String city;
    private String district;
    private String fullAddress;
    private String phoneNumber;
    private String zipCode;
}
