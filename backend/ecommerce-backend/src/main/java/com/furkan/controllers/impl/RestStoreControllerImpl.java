package com.furkan.controllers.impl;

import com.furkan.controllers.IRestStoreController;
import com.furkan.controllers.RestBaseController;
import com.furkan.dto.request.DtoStoreRequest;
import com.furkan.dto.response.DtoStore;
import com.furkan.services.IStoreService;
import com.furkan.utils.RootEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/stores")
@RequiredArgsConstructor
public class RestStoreControllerImpl extends RestBaseController implements IRestStoreController {

    private final IStoreService storeService;

    @PostMapping()
    @Override
    public RootEntity<DtoStore> createStore(@RequestBody DtoStoreRequest input) {
        return ok(storeService.createStore(input));
    }

    @GetMapping("/{id}")
    @Override
    public RootEntity<DtoStore> findStoreById(@PathVariable Long id) {
        return ok(storeService.findStoreById(id));
    }

    @GetMapping()
    @Override
    public RootEntity<List<DtoStore>> findAllStores() {
        return ok(storeService.findAllStores());
    }

    @PutMapping("/{id}")
    @Override
    public RootEntity<DtoStore> updateStoreById(@PathVariable Long id, @RequestBody DtoStoreRequest input) {
        return ok(storeService.updateStoreById(id, input));
    }

    @DeleteMapping("/{id}")
    @Override
    public RootEntity<Void> deleteStoreById(@PathVariable Long id) {
        storeService.deleteStoreById(id);
        return ok();
    }
}
