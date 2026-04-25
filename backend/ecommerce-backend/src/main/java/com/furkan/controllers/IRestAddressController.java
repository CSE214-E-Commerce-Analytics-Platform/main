package com.furkan.controllers;

import com.furkan.dto.request.DtoAddressRequest;
import com.furkan.dto.response.DtoAddress;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import com.furkan.utils.RootEntity;
import org.springframework.security.core.userdetails.UserDetails;


public interface IRestAddressController {
    RootEntity<DtoAddress> createAddress(UserDetails userDetails, DtoAddressRequest request);
    RootEntity<RestPageableEntity<DtoAddress>> findMyAddresses(UserDetails userDetails, RestPageableRequest request);
    RootEntity<DtoAddress> updateAddress(Long addressId, UserDetails userDetails, DtoAddressRequest request);
    RootEntity<Void> deleteAddress(Long addressId, UserDetails userDetails);
}
