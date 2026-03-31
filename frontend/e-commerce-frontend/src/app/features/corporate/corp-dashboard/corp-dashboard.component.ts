import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule, ReactiveFormsModule, FormBuilder, FormGroup, Validators } from '@angular/forms';
import { StoreService } from '../../../core/services/store.service';
import { Store } from '../../../shared/models/store';

@Component({
  selector: 'app-corp-dashboard',
  imports: [CommonModule, FormsModule, ReactiveFormsModule],
  templateUrl: './corp-dashboard.component.html',
  styleUrl: './corp-dashboard.component.css'
})
export class CorpDashboardComponent implements OnInit {

  stores: Store[] = [];
  isLoading: boolean = false;
  storeForm: FormGroup;
  showCreateModal: boolean = false;

  constructor(
    private storeService: StoreService,
    private fb: FormBuilder
  ) {
    this.storeForm = this.fb.group({
      name: ['', [Validators.required, Validators.minLength(3)]]
    });
  }

  ngOnInit(): void {
    this.loadStores();
  }

  loadStores(): void {
    this.isLoading = true;
    this.storeService.getMyStores().subscribe({
      next: (data) => {
        this.stores = data || [];
        this.isLoading = false;
      },
      error: (err) => {
        console.error('Error fetching stores', err);
        this.isLoading = false;
      }
    });
  }

  openCreateModal(): void {
    this.storeForm.reset();
    this.showCreateModal = true;
  }

  closeCreateModal(): void {
    this.showCreateModal = false;
  }

  createStore(): void {
    if (this.storeForm.invalid) return;

    const request = { name: this.storeForm.value.name };
    this.storeService.createStore(request).subscribe({
      next: (newStore) => {
        this.stores.push(newStore);
        this.closeCreateModal();
      },
      error: (err) => {
        console.error('Error creating store', err);
        alert('An error occurred while creating the store.');
      }
    });
  }
}

