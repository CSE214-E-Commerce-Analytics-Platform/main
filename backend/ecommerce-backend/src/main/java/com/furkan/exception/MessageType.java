package com.furkan.exception;

import lombok.Getter;

@Getter
public enum MessageType {

    // Auth
    REFRESH_TOKEN_IS_EXPIRED("1009", "Refresh token is expired or revoked, please login the system again."),
    USER_NOT_FOUND("1001", "With this ID user not found"),
    USER_NOT_FOUND_BY_EMAIL("1002", "With this email user not found"),
    EMAIL_ALREADY_EXISTS("1008", "This email already registered in the system."),
    REFRESH_TOKEN_NOT_FOUND("1003", "Refresh token not found."),
    INVALID_EMAIL_OR_PASSWORD("1004", "Password or Email is incorrect. Please check the authentication information."),
    ACCOUNT_IS_NOT_VERIFIED("1005", "Your account is not verified yet please verify your account before login."),
    SAME_PASSWORD("1006", "Your new password must not be the same as the old password."),

    // VerificationToken
    INVALID_VERIFICATION_TOKEN("1007", "Invalid verification token!"),
    TOKEN_HAS_EXPIRED("1008", "Token has expired!"),
    TOKEN_HAS_ALREADY_BEEN_USED("1009", "Token has already been used."),

    // Corporate Upgrade Request
    UPGRADE_ALREADY_REQUESTED("1008", "You already have a request in pending status."),
    UPGRADE_REQUEST_NOT_FOUND("1009", "The user's request for a corporate account could not be found."),
    UPGRADE_REQUEST_NOT_FOUND_BY_ID("1010", "The user's request for a corporate account could not be found with this ID."),
    UPGRADE_REQUEST_NOT_FOUND_BY_EMAIL("1011", "The user's request for a corporate account could not be found with this email."),
    NOT_PENDING_REQUEST("1012", "Only requests with a pending status can be evaluated"),

    // Category
    CATEGORY_NOT_FOUND("2001", "Selected category not found."),
    PARENT_NOT_FOUND("2006", "No selected parent category was found!"),
    CHILD_CAT_EXISTS("2004", "This category has children categories, first delete these!"),

    // Product
    PRODUCT_NOT_FOUND("3001", "Product not found!"),
    SKU_ALREADY_EXISTS("3005", "This SKU is already exists! Please change the SKU value for this product."),
    UNAUTHORIZED_TRANSACTION("3007", "This product does not belong in your store!"),

    // Store
    STORE_NOT_FOUND("4001", "Store not found!"),
    OWNER_NOT_FOUND("4002", "The owner of this store could not be found."),
    STORE_CORPORATE_AUTH("4003", "Only Corporate users can create a store!"),
    STORE_OWNER_MISMATCH("4004", "You are not owner of this store!"),

    GENERAL_EXCEPTION("9999", "A general error occur.");

    final String code;

    final String message;

    MessageType(String code, String message) {
        this.code = code;
        this.message = message;
    }
}
