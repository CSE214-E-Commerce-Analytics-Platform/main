package com.furkan.controllers;

import com.furkan.dto.request.DtoAddressRequest;
import com.furkan.dto.response.DtoAddress;
import com.furkan.utils.RootEntity;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.List;

public interface IRestAddressController {
    RootEntity<DtoAddress> createAddress(UserDetails userDetails, DtoAddressRequest request);
    RootEntity<List<DtoAddress>> findMyAddresses(UserDetails userDetails);
    RootEntity<DtoAddress> updateAddress(Long addressId, UserDetails userDetails, DtoAddressRequest request);
    RootEntity<Void> deleteAddress(Long addressId, UserDetails userDetails);
}
