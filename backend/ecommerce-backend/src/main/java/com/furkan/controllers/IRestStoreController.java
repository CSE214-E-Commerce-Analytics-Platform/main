package com.furkan.controllers;

import com.furkan.dto.request.DtoStoreRequest;
import com.furkan.dto.response.DtoStore;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import com.furkan.utils.RootEntity;
import org.springframework.security.core.userdetails.UserDetails;

public interface IRestStoreController {

    RootEntity<DtoStore> createStore(DtoStoreRequest input, UserDetails userDetails);

    RootEntity<DtoStore> findStoreById(Long id);

    RootEntity<RestPageableEntity<DtoStore>> findAllStores(RestPageableRequest request);

    RootEntity<DtoStore> updateStoreById(Long id, DtoStoreRequest input, UserDetails userDetails);

    RootEntity<Void> deleteStoreById(Long id, UserDetails userDetails);

    RootEntity<RestPageableEntity<DtoStore>> findMyStores(UserDetails userDetails, RestPageableRequest request);
}
