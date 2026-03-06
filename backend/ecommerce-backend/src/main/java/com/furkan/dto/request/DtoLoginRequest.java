package com.furkan.dto.request;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class DtoLoginRequest {
    @Email(message = "Please enter a valid email.")
    private String email;
    @NotBlank(message = "Password must not be empty.")
    private String password;
}
