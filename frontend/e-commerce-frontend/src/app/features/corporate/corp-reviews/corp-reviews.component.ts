import { Component, inject, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { StoreService } from '../../../core/services/store.service';
import { ReviewService } from '../../../core/services/review.service';
import { ToastService } from '../../../core/services/toast.service';
import { DtoReview } from '../../../shared/models/review';
import { Store } from '../../../shared/models/store';
import { catchError } from 'rxjs/operators';
import { of } from 'rxjs';

@Component({
  selector: 'app-corp-reviews',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './corp-reviews.component.html',
  styleUrl: './corp-reviews.component.css'
})
export class CorpReviewsComponent implements OnInit {
  private storeService = inject(StoreService);
  private reviewService = inject(ReviewService);
  private toastService = inject(ToastService);

  stores: Store[] = [];
  selectedStoreId: number | null = null;
  reviews: DtoReview[] = [];
  isLoading = false;

  ngOnInit(): void {
    this.storeService.getMyStores().subscribe({
      next: (stores) => {
        this.stores = stores || [];
        if (this.stores.length > 0 && this.stores[0].id) {
          this.selectedStoreId = this.stores[0].id;
          this.loadReviews();
        }
      }
    });
  }

  onStoreChange(): void {
    this.reviews = [];
    this.loadReviews();
  }

  loadReviews(): void {
    if (!this.selectedStoreId) return;
    this.isLoading = true;
    this.reviewService.getCorporateReviews(this.selectedStoreId).pipe(
      catchError(err => {
        this.toastService.showError('Failed to load store reviews. ' + (err.error?.exception?.message || ''));
        this.isLoading = false;
        return of([]);
      })
    ).subscribe(reviews => {
      this.reviews = reviews;
      this.isLoading = false;
    });
  }

  getStarsArray(rating: number): number[] {
    return Array(5).fill(0).map((x, i) => i + 1);
  }
}
