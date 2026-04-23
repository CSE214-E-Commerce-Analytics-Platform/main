package com.furkan.services;

import com.furkan.dto.request.DtoCustomerProfileRequest;
import com.furkan.dto.response.DtoCustomerProfile;
import com.furkan.enums.MembershipType;

public interface ICustomerProfileService {

    DtoCustomerProfile findProfileByUserId(Long userId);

    DtoCustomerProfile updateProfileByUserId(Long userId, DtoCustomerProfileRequest request);

    DtoCustomerProfile upgradeMembershipType(Long userId, MembershipType type);
}
