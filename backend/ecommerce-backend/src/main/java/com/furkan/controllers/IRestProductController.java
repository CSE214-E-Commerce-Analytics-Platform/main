package com.furkan.controllers;

import com.furkan.dto.request.DtoProductRequest;
import com.furkan.dto.response.DtoProduct;
import com.furkan.utils.RootEntity;

import java.util.List;

public interface IRestProductController {

    RootEntity<DtoProduct> createProduct(DtoProductRequest input);

    RootEntity<DtoProduct> findProductById(Long id);

    RootEntity<List<DtoProduct>> findAllProducts();

    RootEntity<DtoProduct> updateProductById(Long id, DtoProductRequest input);

    RootEntity<Void> deleteProductById(Long id);

    RootEntity<List<DtoProduct>> findAllByStoreId(Long storeId);
}
