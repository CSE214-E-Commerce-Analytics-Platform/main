package com.furkan.dto.request;

import jakarta.validation.constraints.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DtoRegisterRequest {
    @Email(message = "Please enter a valid email.")
    private String email;

    @NotBlank(message = "Password must not be empty.")
    @Size(min = 6, message = "Password must be at least 6 characters.")
    private String password;

    private int age;

    private String city;

    private String state;

    private String country;
}
