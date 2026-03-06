package com.furkan.controllers;

import com.furkan.dto.request.DtoStoreRequest;
import com.furkan.dto.response.DtoStore;
import com.furkan.utils.RootEntity;

import java.util.List;

public interface IRestStoreController {

    RootEntity<DtoStore> createStore(DtoStoreRequest input);

    RootEntity<DtoStore> findStoreById(Long id);

    RootEntity<List<DtoStore>> findAllStores();

    RootEntity<DtoStore> updateStoreById(Long id, DtoStoreRequest input);

    RootEntity<Void> deleteStoreById(Long id);
}
