package com.furkan.controllers;

import com.furkan.utils.RootEntity;

public class RestBaseController {

    public <T> RootEntity<T> ok(T payload) {
        return RootEntity.ok(payload);
    }

    public <T> RootEntity<T> error(String errorMessage) {
        return RootEntity.error(errorMessage);
    }

    public RootEntity<Void> ok() {
        return RootEntity.ok();
    }
}
