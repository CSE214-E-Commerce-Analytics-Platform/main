package com.furkan.services.impl;

import com.furkan.dto.request.DtoCustomerProfileRequest;
import com.furkan.dto.response.DtoCustomerProfile;
import com.furkan.entities.CustomerProfile;
import com.furkan.entities.User;
import com.furkan.enums.MembershipType;
import com.furkan.exception.BaseException;
import com.furkan.exception.ErrorMessage;
import com.furkan.exception.MessageType;
import com.furkan.repositories.CustomerProfileRepository;
import com.furkan.repositories.UserRepository;
import com.furkan.services.ICustomerProfileService;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
public class CustomerProfileServiceImpl implements ICustomerProfileService {

    private final CustomerProfileRepository profileRepository;
    private final UserRepository userRepository;

    @Override
    public DtoCustomerProfile findProfileByUserId(Long userId) {
        CustomerProfile profile = profileRepository.findByUserId(userId)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.PROFILE_NOT_FOUND, null)));
        return dtoConverter(profile);
    }

    @Override
    @Transactional
    public DtoCustomerProfile updateProfileByUserId(Long userId, DtoCustomerProfileRequest request) {
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.USER_NOT_FOUND, userId.toString())));

        CustomerProfile profile = profileRepository.findByUserId(userId).orElse(new CustomerProfile());

        if (profile.getId() == null) {
            profile.setUser(user);
            profile.setMembershipType(MembershipType.STANDARD); // Varsayılan üyelik tipi
            profile.setCreatedAt(LocalDateTime.now());
        }

        profile.setAge(request.getAge());
        profile.setCity(request.getCity());
        profile.setState(request.getState());
        profile.setCountry(request.getCountry());
        profile.setUpdatedAt(LocalDateTime.now());

        CustomerProfile savedProfile = profileRepository.save(profile);
        return dtoConverter(savedProfile);
    }

    @Override
    @Transactional
    public DtoCustomerProfile upgradeMembershipType(Long userId, MembershipType type) {
        CustomerProfile cp = profileRepository.findByUserId(userId)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.PROFILE_NOT_FOUND, null)));

        MembershipType currentType = cp.getMembershipType();

        if (currentType == type) {
            return dtoConverter(cp);
        }

        if (type.ordinal() < currentType.ordinal()) {
            throw new BaseException(new ErrorMessage(MessageType.CAN_NOT_DOWN_GRADE, null));
        }

        cp.setMembershipType(type);

        CustomerProfile updated = profileRepository.save(cp);

        return dtoConverter(updated);
    }

    private DtoCustomerProfile dtoConverter(CustomerProfile profile) {
        DtoCustomerProfile dto = new DtoCustomerProfile();
        BeanUtils.copyProperties(profile, dto);
        dto.setUserId(profile.getUser().getId());
        return dto;
    }
}
