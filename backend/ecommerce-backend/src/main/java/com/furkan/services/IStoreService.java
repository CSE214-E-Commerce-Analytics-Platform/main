package com.furkan.services;

import com.furkan.dto.request.DtoStoreRequest;
import com.furkan.dto.response.DtoStore;
import com.furkan.entities.User;

import java.util.List;

public interface IStoreService {

    DtoStore createStore(DtoStoreRequest input, Long authenticatedUserId);

    DtoStore findStoreById(Long id);

    List<DtoStore> findAllStores();

    DtoStore updateStoreById(Long id, DtoStoreRequest input, Long authenticatedUserId);

    void deleteStoreById(Long id, Long authenticatedUserId);

    List<DtoStore> findMyStores(Long authenticatedUserId);

    DtoStore createStoreForCorporateUpgradeRole(User user, DtoStoreRequest input);
}
