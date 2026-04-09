import { Component, inject, OnInit } from '@angular/core';
import { AuthService } from '../../../core/services/auth.service';
import { Router, RouterLink } from '@angular/router';
import { ReactiveFormsModule, FormBuilder, Validators } from '@angular/forms';
import { extractErrorMessage } from '../../../core/utils/error.util';

@Component({
  selector: 'app-login',
  imports: [ReactiveFormsModule, RouterLink],
  templateUrl: './login.component.html',
  styleUrl: './login.component.css'
})
export class LoginComponent implements OnInit {
  private fb = inject(FormBuilder);
  private authService = inject(AuthService);
  private router = inject(Router);

  loginForm = this.fb.group({
    email: ['', [Validators.required, Validators.email]],
    password: ['', [Validators.required, Validators.minLength(6)]]
  });

  isLoading = false;
  showPassword = false;
  loginError: string | null = null;

  ngOnInit(): void { }

  onLogin() {
    if (this.loginForm.valid) {
      this.isLoading = true;
      this.loginError = null;
      const { email, password } = this.loginForm.value as { email: string; password: string };

      this.authService.login(email, password).subscribe({
        next: () => {
          this.isLoading = false;
          const role = this.authService.getRole();
          if (role === 'ADMIN') {
            this.router.navigate(['/admin/dashboard']);
          } else if (role === 'CORPORATE') {
            this.router.navigate(['/corporate/dashboard']);
          } else {
            this.router.navigate(['/individual/products']);
          }
        },
        error: (err) => {
          this.isLoading = false;
          this.loginError = extractErrorMessage(err, 'Login failed. Please check your credentials.');
          this.loginForm.reset();
        }
      });
    }
  }
}

