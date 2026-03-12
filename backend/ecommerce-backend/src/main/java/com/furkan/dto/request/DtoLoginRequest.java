package com.furkan.dto.request;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.Data;

@Data
public class DtoLoginRequest {
    @Email(message = "Please enter a valid email.")
    private String email;
    @NotBlank(message = "Password must not be empty.")
    @Size(min = 6, message = "Password must be at least 6 characters.")
    private String password;
}
