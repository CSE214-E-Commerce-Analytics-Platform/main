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

  corp_email: String | null = this.authService.getCurrentUserEmail();
  corp_email_parsed = this.corp_email?.split('@')[0];

  logout() {
    this.authService.logout().subscribe(() => {
      this.router.navigate(['/login']);
    });
  }
}
