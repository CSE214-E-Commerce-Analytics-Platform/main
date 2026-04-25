package com.furkan.services;

import com.furkan.dto.request.DtoCategoryRequest;
import com.furkan.dto.response.DtoCategory;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;

import java.util.List;

public interface ICategoryService {

    DtoCategory createCategory(DtoCategoryRequest input);

    DtoCategory findCategoryById(Long id);

    RestPageableEntity<DtoCategory> findAllCategories(RestPageableRequest request);

    DtoCategory updateCategoryById(Long id, DtoCategoryRequest input);

    void deleteCategoryById(Long id);
}
