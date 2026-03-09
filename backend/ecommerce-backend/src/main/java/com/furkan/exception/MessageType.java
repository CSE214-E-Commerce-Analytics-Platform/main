package com.furkan.exception;

import lombok.Getter;

@Getter
public enum MessageType {

    REFRESH_TOKEN_IS_EXPIRED("1009", "Refresh token is expired, please login the system."),
    USER_NOT_FOUND("1001", "with ID user not found"),
    EMAIL_ALREADY_EXISTS("1008", "This email already registered in the system."),
    EMAIL_NOT_FOUND("1009", "E mail not found."),
    REFRESH_TOKEN_NOT_FOUND("1003", "Refresh token not found."),
    INVALID_PASSWORD("1004", "Password is incorrect."),

    GENERAL_EXCEPTION("9999", "A general error occur.");

    final String code;

    final String message;

    MessageType(String code, String message) {
        this.code = code;
        this.message = message;
    }
}
