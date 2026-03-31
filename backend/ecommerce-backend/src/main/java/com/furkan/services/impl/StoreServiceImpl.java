package com.furkan.services.impl;

import com.furkan.dto.request.DtoStoreRequest;
import com.furkan.dto.response.DtoStore;
import com.furkan.entities.Store;
import com.furkan.entities.User;
import com.furkan.enums.RoleType;
import com.furkan.repositories.StoreRepository;
import com.furkan.repositories.UserRepository;
import com.furkan.services.IStoreService;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
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
                .orElseThrow(() -> new RuntimeException("Owner (User) not found!"));

        if (owner.getRoleType() != RoleType.CORPORATE) {
            throw new RuntimeException("Only Corporate users can create a store! Your role: " + owner.getRoleType());
        }

        Store store = new Store();
        store.setName(input.getName());
        store.setStatus("ACTIVE");
        store.setOwner(owner);

        return dtoTransformation(storeRepository.save(store));
    }

    private Store getStoreIfOwner(Long storeId, Long userId) {
        Store store = storeRepository.findByIdAndOwnerId(storeId, userId)
                .orElseThrow(() -> new RuntimeException("Store not found!"));

        if (!store.getOwner().getId().equals(userId)) {
            throw new RuntimeException("You are not authorized to perform this transaction! The store does not belong to you.");
        }

        return store;
    }

    @Override
    public DtoStore findStoreById(Long id) {
        Store store = storeRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Store not found!"));
        return dtoTransformation(store);
    }

    @Override
    public List<DtoStore> findAllStores() {
        return storeRepository.findAll().stream()
                .map(this::dtoTransformation)
                .toList();
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
    public List<DtoStore> findMyStores(Long authenticatedUserId) {
        List<Store> myStores = storeRepository.findAllByOwnerId(authenticatedUserId);

        if (myStores.isEmpty()) {
            return new ArrayList<>();
        }

        return myStores.stream()
                .map(this::dtoTransformation)
                .toList();
    }
}
