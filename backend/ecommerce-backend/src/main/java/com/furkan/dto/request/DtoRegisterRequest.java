package com.furkan.dto.request;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
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
    private String password;

    private String gender;
}
