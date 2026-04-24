package com.furkan.services.impl;

import com.furkan.dto.request.DtoStoreRequest;
import com.furkan.dto.response.DtoStore;
import com.furkan.entities.Store;
import com.furkan.entities.User;
import com.furkan.enums.RoleType;
import com.furkan.exception.BaseException;
import com.furkan.exception.ErrorMessage;
import com.furkan.exception.MessageType;
import com.furkan.repositories.StoreRepository;
import com.furkan.repositories.UserRepository;
import com.furkan.services.IStoreService;
import com.furkan.utils.PagerUtil;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.BeanUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class StoreServiceImpl implements IStoreService {

    private final StoreRepository storeRepository;
    private final UserRepository userRepository;

    private DtoStore dtoTransformation(Store store) {
        DtoStore dto = new DtoStore();
        BeanUtils.copyProperties(store, dto);
        if (store.getOwner() != null) {
            dto.setOwnerId(store.getOwner().getId());
        }
        return dto;
    }

    @Override
    @Transactional
    public DtoStore createStore(DtoStoreRequest input, Long authenticatedUserId) {
        User owner = userRepository.findById(authenticatedUserId)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.OWNER_NOT_FOUND, authenticatedUserId.toString())));

        if (owner.getRoleType() != RoleType.CORPORATE) {
            throw new BaseException(new ErrorMessage(MessageType.STORE_CORPORATE_AUTH, authenticatedUserId.toString()));
        }

        Store store = new Store();
        store.setName(input.getName());
        store.setStatus("ACTIVE");
        store.setOwner(owner);

        return dtoTransformation(storeRepository.save(store));
    }

    private Store getStoreIfOwner(Long storeId, Long userId) {
        Store store = storeRepository.findByIdAndOwnerId(storeId, userId)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.STORE_NOT_FOUND, storeId.toString())));

        if (!store.getOwner().getId().equals(userId)) {
            throw new BaseException(new ErrorMessage(MessageType.STORE_OWNER_MISMATCH, storeId.toString()));
        }

        return store;
    }

    @Override
    public DtoStore findStoreById(Long id) {
        Store store = storeRepository.findById(id)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.STORE_NOT_FOUND, id.toString())));
        return dtoTransformation(store);
    }

    @Override
    public RestPageableEntity<DtoStore> findAllStores(RestPageableRequest request) {
        if (request.getColumnName() == null || request.getColumnName().isEmpty()) {
            request.setColumnName("id");
            request.setAsc(false);
        }

        Pageable pageable = PagerUtil.toPageable(request);

        Page<Store> storePage = storeRepository.findAll(pageable);

        List<DtoStore> dtoList = storePage.getContent().stream()
                .map(this::dtoTransformation)
                .toList();

        return PagerUtil.toPageableResponse(storePage, dtoList);
    }

    @Override
    @Transactional
    public DtoStore updateStoreById(Long id, DtoStoreRequest input, Long authenticatedUserId) {
        Store store = getStoreIfOwner(id, authenticatedUserId);
        store.setName(input.getName());
        return dtoTransformation(storeRepository.save(store));
    }

    @Override
    @Transactional
    public void deleteStoreById(Long id, Long authenticatedUserId) {
        Store store = getStoreIfOwner(id, authenticatedUserId);
        storeRepository.delete(store);
    }

    @Override
    public RestPageableEntity<DtoStore> findMyStores(Long authenticatedUserId, RestPageableRequest request) {
        if (request.getColumnName() == null || request.getColumnName().isEmpty()) {
            request.setColumnName("id");
            request.setAsc(false);
        }

        Pageable pageable = PagerUtil.toPageable(request);

        Page<Store> storePage = storeRepository.findAllByOwnerId(authenticatedUserId, pageable);

        List<DtoStore> dtoList = storePage.getContent().stream()
                .map(this::dtoTransformation)
                .toList();

        return PagerUtil.toPageableResponse(storePage, dtoList);
    }

    @Override
    @Transactional
    public DtoStore createStoreForCorporateUpgradeRole(User user, DtoStoreRequest input) {
        Store store = new Store();
        store.setName(input.getName());
        store.setStatus("ACTIVE");
        store.setOwner(user);

        return dtoTransformation(storeRepository.save(store));
    }
}
