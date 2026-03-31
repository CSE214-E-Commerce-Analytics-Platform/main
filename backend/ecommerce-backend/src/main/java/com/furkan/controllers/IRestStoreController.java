package com.furkan.controllers;

import com.furkan.dto.request.DtoStoreRequest;
import com.furkan.dto.response.DtoStore;
import com.furkan.utils.RootEntity;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.List;

public interface IRestStoreController {

    RootEntity<DtoStore> createStore(DtoStoreRequest input, UserDetails userDetails);

    RootEntity<DtoStore> findStoreById(Long id);

    RootEntity<List<DtoStore>> findAllStores();

    RootEntity<DtoStore> updateStoreById(Long id, DtoStoreRequest input, UserDetails userDetails);

    RootEntity<Void> deleteStoreById(Long id, UserDetails userDetails);

    RootEntity<List<DtoStore>> findMyStores(UserDetails userDetails);
}
