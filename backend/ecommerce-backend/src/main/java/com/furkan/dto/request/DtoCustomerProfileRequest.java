package com.furkan.dto.request;

import lombok.Data;

@Data
public class DtoCustomerProfileRequest {
    private Integer age;
    private String city;
    private String state;
    private String country;
}
