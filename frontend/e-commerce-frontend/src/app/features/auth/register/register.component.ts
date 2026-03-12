import { Component, inject } from '@angular/core';
import { AuthService } from '../../../core/services/auth.service';
import { Router, RouterLink } from '@angular/router';
import { ReactiveFormsModule, FormBuilder, Validators } from '@angular/forms';

@Component({
  selector: 'app-register',
  imports: [ReactiveFormsModule, RouterLink],
  templateUrl: './register.component.html',
  styleUrl: './register.component.css'
})
export class RegisterComponent {
  private fb = inject(FormBuilder);
  private authService = inject(AuthService);
  private router = inject(Router);

  registerForm = this.fb.group({
    email: ['', [Validators.required, Validators.email]],
    password: ['', [Validators.required, Validators.minLength(6)]],
    confirmPassword: ['', [Validators.required]],
    gender: ['', [Validators.required]]
  });

  errorMessage = '';
  successMessage = '';
  isLoading = false;
  showPassword = false;
  showConfirmPassword = false;

  onRegister() {
    this.errorMessage = '';
    this.successMessage = '';
    this.isLoading = true;

    if (this.registerForm.valid) {
      const { email, password, confirmPassword, gender } = this.registerForm.value as {
        email: string;
        password: string;
        confirmPassword: string;
        gender: string;
      };

      if (password !== confirmPassword) {
        this.errorMessage = 'Passwords do not match.';
        return;
      }

      this.authService.register(email, password, gender).subscribe({
        next: (message: string) => {
          this.successMessage = message || 'Registration successful. Please check your email to verify your account.';
          this.registerForm.reset();
          this.isLoading = false;
        },
        error: (err) => {
          this.errorMessage = err?.error?.message || err?.message || 'Registration failed. Please try again.';
          this.isLoading = false;
        }
      });
    }
  }
}
