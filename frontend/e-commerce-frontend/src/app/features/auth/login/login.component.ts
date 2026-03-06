import { Component, inject, OnInit } from '@angular/core';
import { AuthService } from '../../../core/services/auth.service';
import { Router } from '@angular/router';
import { ReactiveFormsModule, FormBuilder, Validators } from '@angular/forms';

@Component({
  selector: 'app-login',
  imports: [ReactiveFormsModule],
  templateUrl: './login.component.html',
  styleUrl: './login.component.css'
})
export class LoginComponent implements OnInit {
  private fb = inject(FormBuilder);
  private authService = inject(AuthService);
  private router = inject(Router);

  // Form yapısını ve kurallarını (Validators) tanımlıyoruz
  loginForm = this.fb.group({
    email: ['', [Validators.required, Validators.email]], // Email formatı kontrolü
    password: ['', [Validators.required, Validators.minLength(6)]] // En az 6 karakter
  });

  ngOnInit(): void {

  }

  onLogin() {
    if (this.loginForm.valid) {
      // Form geçerliyse backend'e gönderiyoruz

      const credentials = this.loginForm.value as { email: string; password: string };

      this.authService.login(credentials).subscribe({
        next: (user) => {
          console.log('Giriş başarılı!');
        },
        error: (err) => alert('Giriş yapılamadı. Bilgilerinizi kontrol edin.')
      });
    }
  }
}
