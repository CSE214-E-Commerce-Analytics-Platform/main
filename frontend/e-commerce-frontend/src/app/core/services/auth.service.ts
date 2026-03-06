import { Injectable, signal } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Router } from '@angular/router';
import { User } from '../../shared/models/user';
import { ApiResponse } from '../../shared/models/api-response';
import { tap, map } from 'rxjs';
import { environment } from '../../../environments/environment.development';

@Injectable({
  providedIn: 'root'
})
export class AuthService {

  currentUser = signal<User | null>(null);

  private readonly apiUrl = `${environment.baseUrl}/users`;

  constructor(private http: HttpClient, private router: Router) { }

  login(credentials: { email: string, password: string }) {
    return this.http.post<ApiResponse<User>>(`${this.apiUrl}/login`, credentials).pipe(
      map(res => res.payload as User),
      tap(user => {
        this.currentUser.set(user);
        this.router.navigate(['/products']);
      })
    );
  }

  logout() {
    this.currentUser.set(null);
    this.router.navigate(['/login']);
  }

  isLoggedIn(): boolean {
    return this.currentUser() !== null;
  }
}
