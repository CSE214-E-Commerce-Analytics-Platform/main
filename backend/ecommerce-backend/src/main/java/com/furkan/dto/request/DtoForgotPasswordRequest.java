package com.furkan.dto.request;

import jakarta.validation.constraints.Email;
import lombok.Data;

@Data
public class DtoForgotPasswordRequest {

    @Email(message = "Please enter a valid email.")
    private String email;
}
