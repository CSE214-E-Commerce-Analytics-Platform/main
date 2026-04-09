import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterLink } from '@angular/router';
import { StoreService } from '../../../core/services/store.service';
import { Store } from '../../../shared/models/store';

@Component({
  selector: 'app-ind-stores',
  standalone: true,
  imports: [CommonModule, RouterLink],
  templateUrl: './ind-stores.component.html',
  styleUrl: './ind-stores.component.css'
})
export class IndStoresComponent implements OnInit {
  private storeService = inject(StoreService);

  stores: Store[] = [];
  isLoading = true;
  errorMessage = '';

  ngOnInit(): void {
    this.fetchStores();
  }

  fetchStores(): void {
    this.isLoading = true;
    this.storeService.getAllStores().subscribe({
      next: (data) => {
        this.stores = data;
        this.isLoading = false;
      },
      error: (err) => {
        console.error('Error fetching stores', err);
        this.errorMessage = 'Failed to load stores. Please try again later.';
        this.isLoading = false;
      }
    });
  }
}
