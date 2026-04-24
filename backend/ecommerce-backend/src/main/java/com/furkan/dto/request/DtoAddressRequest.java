package com.furkan.dto.request;

import lombok.Data;

@Data
public class DtoAddressRequest {
    private String city;
    private String district;
    private String fullAddress;
    private String phoneNumber;
    private String zipCode;
}
