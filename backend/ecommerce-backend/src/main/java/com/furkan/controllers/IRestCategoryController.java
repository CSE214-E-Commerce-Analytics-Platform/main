package com.furkan.controllers;

import com.furkan.dto.request.DtoCategoryRequest;
import com.furkan.dto.response.DtoCategory;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import com.furkan.utils.RootEntity;

import java.util.List;

public interface IRestCategoryController {

    RootEntity<DtoCategory> createCategory(DtoCategoryRequest input);

    RootEntity<DtoCategory> findCategoryById(Long id);

    RootEntity<RestPageableEntity<DtoCategory>> findAllCategories(RestPageableRequest request);

    RootEntity<DtoCategory> updateCategoryById(Long id, DtoCategoryRequest input);

    RootEntity<Void> deleteCategoryById(Long id);
}
