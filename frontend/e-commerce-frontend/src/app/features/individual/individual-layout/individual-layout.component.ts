import { Component, inject } from '@angular/core';
import { Router, RouterLink, RouterOutlet } from '@angular/router';
import { AuthService } from '../../../core/services/auth.service';

@Component({
  selector: 'app-individual-layout',
  imports: [RouterOutlet, RouterLink],
  templateUrl: './individual-layout.component.html',
  styleUrl: './individual-layout.component.css'
})
export class IndividualLayoutComponent {
  private authService = inject(AuthService);
  private router = inject(Router);

  logout() {
    this.authService.logout().subscribe(() => {
      this.router.navigate(['/login']);
    });
  }
}
