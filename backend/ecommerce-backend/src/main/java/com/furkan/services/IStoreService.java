package com.furkan.services;

import com.furkan.dto.request.DtoStoreRequest;
import com.furkan.dto.response.DtoStore;
import com.furkan.entities.User;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;

public interface IStoreService {

    DtoStore createStore(DtoStoreRequest input, Long authenticatedUserId);

    DtoStore findStoreById(Long id);

    RestPageableEntity<DtoStore> findAllStores(RestPageableRequest request);

    DtoStore updateStoreById(Long id, DtoStoreRequest input, Long authenticatedUserId);

    void deleteStoreById(Long id, Long authenticatedUserId);

    RestPageableEntity<DtoStore> findMyStores(Long authenticatedUserId, RestPageableRequest request);

    DtoStore createStoreForCorporateUpgradeRole(User user, DtoStoreRequest input);
}
