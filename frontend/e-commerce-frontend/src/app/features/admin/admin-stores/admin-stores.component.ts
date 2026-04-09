import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { StoreService } from '../../../core/services/store.service';
import { Store } from '../../../shared/models/store';

@Component({
  selector: 'app-admin-stores',
  standalone: true,
  imports: [CommonModule, RouterModule],
  templateUrl: './admin-stores.component.html',
  styleUrl: './admin-stores.component.css'
})
export class AdminStoresComponent implements OnInit {

  stores: Store[] = [];
  isLoading = true;
  errorMessage = '';

  constructor(private storeService: StoreService) {}

  ngOnInit(): void {
    this.loadStores();
  }

  loadStores(): void {
    this.isLoading = true;
    this.storeService.getAllStores().subscribe({
      next: (data) => {
        this.stores = data || [];
        this.isLoading = false;
      },
      error: (err) => {
        console.error('Error loading stores', err);
        this.errorMessage = 'Failed to load platform stores.';
        this.isLoading = false;
      }
    });
  }

  deleteStore(id: number): void {
    if (confirm('Are you sure you want to completely delete this store? This will also remove associated products.')) {
      this.storeService.deleteStore(id).subscribe({
        next: () => {
          this.stores = this.stores.filter(s => s.id !== id);
        },
        error: (err) => {
          console.error('Error deleting store', err);
          alert('Failed to delete the store.');
        }
      });
    }
  }
}
