package com.furkan.dto.response;

import com.furkan.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class DtoCustomerProfile extends BaseDto {
    private Integer age;
    private String city;
    private String state;
    private String country;
    private String membershipType;
    private Long userId;
}
