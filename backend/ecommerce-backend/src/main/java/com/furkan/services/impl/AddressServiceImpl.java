package com.furkan.services.impl;

import com.furkan.dto.request.DtoAddressRequest;
import com.furkan.dto.response.DtoAddress;
import com.furkan.entities.Address;
import com.furkan.entities.User;
import com.furkan.exception.BaseException;
import com.furkan.exception.ErrorMessage;
import com.furkan.exception.MessageType;
import com.furkan.repositories.AddressRepository;
import com.furkan.repositories.UserRepository;
import com.furkan.services.IAddressService;
import com.furkan.utils.PagerUtil;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.BeanUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class AddressServiceImpl implements IAddressService {

    private final AddressRepository addressRepository;
    private final UserRepository userRepository;

    @Override
    public DtoAddress createAddress(Long userId, DtoAddressRequest request) {
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new BaseException(
                        new ErrorMessage(MessageType.USER_NOT_FOUND, userId.toString())));

        Address address = new Address();
        address.setUser(user);
        BeanUtils.copyProperties(request, address);

        return dtoConverter(addressRepository.save(address));
    }

    @Override
    public RestPageableEntity<DtoAddress> findMyAddresses(Long userId,RestPageableRequest request) {
        if (request.getColumnName() == null || request.getColumnName().isEmpty()) {
            request.setColumnName("id");
            request.setAsc(false);
        }

        Pageable pageable = PagerUtil.toPageable(request);

        Page<Address> addressPage = addressRepository.findByUserId(userId, pageable);

        List<DtoAddress> dtoList = addressPage.getContent().stream()
                .map(this::dtoConverter)
                .toList();

        return PagerUtil.toPageableResponse(addressPage, dtoList);
    }

    @Override
    public DtoAddress updateAddress(Long addressId, Long userId, DtoAddressRequest request) {
        Address address = addressRepository.findById(addressId)
                .orElseThrow(() -> new BaseException(
                        new ErrorMessage(MessageType.ADDRESS_NOT_FOUND, addressId.toString())));

        if (!address.getUser().getId().equals(userId)) {
            throw new BaseException(
                    new ErrorMessage(MessageType.UNAUTHORIZED, null));
        }

        BeanUtils.copyProperties(request, address);
        return dtoConverter(addressRepository.save(address));
    }

    @Override
    public void deleteAddress(Long addressId, Long userId) {
        Address address = addressRepository.findById(addressId)
                .orElseThrow(() -> new BaseException(
                        new ErrorMessage(MessageType.ADDRESS_NOT_FOUND, addressId.toString())));

        if (!address.getUser().getId().equals(userId)) {
            throw new BaseException(
                    new ErrorMessage(MessageType.UNAUTHORIZED, null));
        }

        addressRepository.delete(address);
    }

    private DtoAddress dtoConverter(Address address) {
        DtoAddress dto = new DtoAddress();
        BeanUtils.copyProperties(address, dto);
        return dto;
    }
}
