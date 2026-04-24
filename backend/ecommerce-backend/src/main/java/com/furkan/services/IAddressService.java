package com.furkan.services;

import com.furkan.dto.request.DtoAddressRequest;
import com.furkan.dto.response.DtoAddress;

import java.util.List;

public interface IAddressService {
    DtoAddress createAddress(Long userId, DtoAddressRequest request);

    List<DtoAddress> findMyAddresses(Long userId);

    DtoAddress updateAddress(Long addressId, Long userId, DtoAddressRequest request);

    void deleteAddress(Long addressId, Long userId);
}
