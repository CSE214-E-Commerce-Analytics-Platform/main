package com.furkan.services;

import com.furkan.dto.request.DtoAddressRequest;
import com.furkan.dto.response.DtoAddress;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;

public interface IAddressService {
    DtoAddress createAddress(Long userId, DtoAddressRequest request);

    RestPageableEntity<DtoAddress> findMyAddresses(Long userId, RestPageableRequest request);

    DtoAddress updateAddress(Long addressId, Long userId, DtoAddressRequest request);

    void deleteAddress(Long addressId, Long userId);
}
