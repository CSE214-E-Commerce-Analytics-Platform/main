import { Component, inject } from '@angular/core';
import { Router, RouterLink, RouterLinkActive } from '@angular/router';
import { CommonModule } from '@angular/common';
import { AuthService } from '../../../core/services/auth.service';

@Component({
    selector: 'app-navbar',
    imports: [CommonModule, RouterLink, RouterLinkActive],
    templateUrl: './navbar.component.html',
    styleUrl: './navbar.component.css'
})
export class NavbarComponent {
    private authService = inject(AuthService);
    private router = inject(Router);

    get email(): string {
        return this.authService.currentUser()?.email ?? '';
    }

    onLogout(): void {
        this.authService.logout();
    }
}
