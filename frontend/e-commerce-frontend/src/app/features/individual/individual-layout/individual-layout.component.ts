import { Component, inject, signal } from '@angular/core';
import { Router, RouterLink, RouterOutlet, RouterLinkActive } from '@angular/router';
import { AuthService } from '../../../core/services/auth.service';
import { ThemeService } from '../../../core/services/theme.service';
import { CommonModule } from '@angular/common';

@Component({
  selector: 'app-individual-layout',
  imports: [RouterOutlet, RouterLink, RouterLinkActive, CommonModule],
  templateUrl: './individual-layout.component.html',
  styleUrl: './individual-layout.component.css'
})
export class IndividualLayoutComponent {
  private authService = inject(AuthService);
  private router = inject(Router);
  themeService = inject(ThemeService);

  ind_email: String | null = this.authService.getCurrentUserEmail();
  ind_email_parsed = this.ind_email?.split('@')[0];

  sidebarOpen = signal(false);

  toggleSidebar() { this.sidebarOpen.update(v => !v); }
  closeSidebar()  { this.sidebarOpen.set(false); }

  logout() {
    this.authService.logout().subscribe(() => {
      this.router.navigate(['/login']);
    });
  }
}
