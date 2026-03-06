package com.furkan.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.Setter;
import org.springframework.http.HttpStatus;

@Getter
@Setter
@JsonInclude(value = JsonInclude.Include.NON_NULL)
public class RootEntity<T> {

    private Integer status;

    private T payload;

    private String errorMessage;

    private String message;

    public static <T> RootEntity<T> ok(T payload) {
        RootEntity<T> rootEntity = new RootEntity<>();
        rootEntity.setStatus(HttpStatus.OK.value());
        rootEntity.setPayload(payload);
        return rootEntity;
    }

    public static <T> RootEntity<T> error(String errorMessage) {
        RootEntity<T> rootEntity = new RootEntity<>();
        rootEntity.setStatus(HttpStatus.INTERNAL_SERVER_ERROR.value());
        rootEntity.setErrorMessage(errorMessage);
        return rootEntity;
    }

    public static RootEntity<Void> ok() {
        RootEntity<Void> response = new RootEntity<>();
        response.setStatus(HttpStatus.OK.value());
        response.setMessage("Success");
        return response;
    }
}
