package com.furkan.services;

import com.furkan.dto.request.DtoCategoryRequest;
import com.furkan.dto.response.DtoCategory;

import java.util.List;

public interface ICategoryService {

    DtoCategory createCategory(DtoCategoryRequest input);

    DtoCategory findCategoryById(Long id);

    List<DtoCategory> findAllCategories();

    DtoCategory updateCategoryById(Long id, DtoCategoryRequest input);

    void deleteCategoryById(Long id);
}
