package com.furkan.controllers;

import com.furkan.dto.request.DtoProductRequest;
import com.furkan.dto.response.DtoProduct;
import com.furkan.utils.RootEntity;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.List;

public interface IRestProductController {

    RootEntity<DtoProduct> createProduct(DtoProductRequest input, UserDetails userDetails);

    RootEntity<DtoProduct> findProductById(Long id);

    RootEntity<List<DtoProduct>> findAllProducts();

    RootEntity<DtoProduct> updateProductById(Long id, DtoProductRequest input, UserDetails userDetails);

    RootEntity<Void> deleteProductById(Long id, UserDetails userDetails);

    RootEntity<List<DtoProduct>> findAllByStoreId(Long storeId);
}
