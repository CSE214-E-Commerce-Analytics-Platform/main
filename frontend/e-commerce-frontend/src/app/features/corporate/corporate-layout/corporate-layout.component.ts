import { Component, inject } from '@angular/core';
import { Router, RouterLink, RouterOutlet } from '@angular/router';
import { AuthService } from '../../../core/services/auth.service';

@Component({
  selector: 'app-corporate-layout',
  imports: [RouterOutlet, RouterLink],
  templateUrl: './corporate-layout.component.html',
  styleUrl: './corporate-layout.component.css'
})
export class CorporateLayoutComponent {
  private authService = inject(AuthService);
  private router = inject(Router);

  logout() {
    this.authService.logout().subscribe(() => {
      this.router.navigate(['/login']);
    });
  }
}
