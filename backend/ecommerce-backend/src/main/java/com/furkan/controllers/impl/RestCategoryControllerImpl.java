package com.furkan.controllers.impl;

import com.furkan.controllers.IRestCategoryController;
import com.furkan.controllers.RestBaseController;
import com.furkan.dto.request.DtoCategoryRequest;
import com.furkan.dto.response.DtoCategory;
import com.furkan.services.ICategoryService;
import com.furkan.utils.RootEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping(value = "/api/categories")
@RequiredArgsConstructor
public class RestCategoryControllerImpl extends RestBaseController implements IRestCategoryController {

    private final ICategoryService categoryService;

    @PostMapping()
    @Override
    public RootEntity<DtoCategory> createCategory(@RequestBody DtoCategoryRequest input) {
        return ok(categoryService.createCategory(input));
    }

    @GetMapping("/{id}")
    @Override
    public RootEntity<DtoCategory> findCategoryById(@PathVariable Long id) {
        return ok(categoryService.findCategoryById(id));
    }

    @GetMapping()
    @Override
    public RootEntity<List<DtoCategory>> findAllCategories() {
        return ok(categoryService.findAllCategories());
    }

    @PutMapping("/{id}")
    @Override
    public RootEntity<DtoCategory> updateCategoryById(@PathVariable Long id, @RequestBody DtoCategoryRequest input) {
        return ok(categoryService.updateCategoryById(id, input));
    }

    @DeleteMapping("/{id}")
    @Override
    public RootEntity<Void> deleteCategoryById(@PathVariable Long id) {
        categoryService.deleteCategoryById(id);
        return ok();
    }
}
