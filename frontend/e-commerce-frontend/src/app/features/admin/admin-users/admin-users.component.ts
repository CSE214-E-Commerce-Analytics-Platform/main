import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { UserService } from '../../../core/services/user.service';
import { User, UserRequest } from '../../../shared/models/user';
import { AuthService } from '../../../core/services/auth.service';

type RoleFilter = 'ALL' | 'INDIVIDUAL' | 'CORPORATE' | 'ADMIN';

@Component({
  selector: 'app-admin-users',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './admin-users.component.html',
  styleUrl: './admin-users.component.css'
})
export class AdminUsersComponent implements OnInit {

  users: User[] = [];
  isLoading = true;
  errorMessage = '';
  currentUserEmail: string | null = null;

  selectedRole: RoleFilter = 'ALL';

  constructor(
    private userService: UserService,
    private authService: AuthService
  ) {}

  ngOnInit(): void {
    this.currentUserEmail = this.authService.getCurrentUserEmail();
    this.loadUsers();
  }

  loadUsers(): void {
    this.isLoading = true;
    this.errorMessage = '';

    const req$ = this.selectedRole === 'ALL'
      ? this.userService.findAllUsers()
      : this.userService.findAllUsersByRole(this.selectedRole);

    req$.subscribe({
      next: (data) => {
        this.users = data || [];
        this.isLoading = false;
      },
      error: (err) => {
        console.error('Error loading users', err);
        this.errorMessage = 'Failed to load users.';
        this.isLoading = false;
      }
    });
  }

  setRoleFilter(role: RoleFilter): void {
    this.selectedRole = role;
    this.loadUsers();
  }

  toggleUserStatus(user: User): void {
    const currentState = user.active !== undefined ? user.active : user.isActive;
    const request: UserRequest = {
      active: !currentState,
      isActive: !currentState
    };

    this.userService.updateUserById(user.id, request).subscribe({
      next: (updatedUser) => {
        const index = this.users.findIndex(u => u.id === updatedUser.id);
        if (index !== -1) {
          this.users[index] = updatedUser;
        }
      },
      error: (err) => {
        console.error('Error updating user', err);
        alert('Failed to update user status.');
      }
    });
  }

  deleteUser(id: number): void {
    if(confirm('Are you sure you want to completely delete this user?')) {
      this.userService.deleteUserById(id).subscribe({
        next: () => {
          this.users = this.users.filter(u => u.id !== id);
        },
        error: (err) => {
          console.error('Error deleting user', err);
          alert('Failed to delete user.');
        }
      });
    }
  }

  getRoleIcon(role: string): string {
    switch (role) {
      case 'ADMIN': return '👑';
      case 'CORPORATE': return '🏢';
      case 'INDIVIDUAL': return '👤';
      default: return '❓';
    }
  }

  isUserActive(user: User): boolean {
    return user.active !== undefined ? !!user.active : !!user.isActive;
  }
}
