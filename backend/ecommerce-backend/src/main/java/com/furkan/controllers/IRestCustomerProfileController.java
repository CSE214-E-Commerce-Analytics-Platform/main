package com.furkan.controllers;

import com.furkan.dto.request.DtoCustomerProfileRequest;
import com.furkan.dto.response.DtoCustomerProfile;
import com.furkan.enums.MembershipType;
import com.furkan.utils.RootEntity;
import org.springframework.security.core.userdetails.UserDetails;

public interface IRestCustomerProfileController {

    RootEntity<DtoCustomerProfile> findProfileByUserId(UserDetails userDetails);

    RootEntity<DtoCustomerProfile> updateProfileByUserId(UserDetails userDetails, DtoCustomerProfileRequest request);

    RootEntity<DtoCustomerProfile> upgradeMembershipType(UserDetails userDetails, MembershipType type);
}
