import { Component, inject, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ReviewService } from '../../../core/services/review.service';
import { ProductService } from '../../../core/services/product.service';
import { StoreService } from '../../../core/services/store.service';
import { ToastService } from '../../../core/services/toast.service';
import { DtoReview, Sentiment } from '../../../shared/models/review';
import { Product } from '../../../shared/models/product';
import { Store } from '../../../shared/models/store';
import { catchError } from 'rxjs/operators';
import { of } from 'rxjs';

@Component({
  selector: 'app-corp-reviews',
  imports: [CommonModule, FormsModule],
  templateUrl: './corp-reviews.component.html',
  styleUrl: './corp-reviews.component.css'
})
export class CorpReviewsComponent implements OnInit {
  private reviewService = inject(ReviewService);
  private productService = inject(ProductService);
  private storeService = inject(StoreService);
  private toastService = inject(ToastService);

  Sentiment = Sentiment;

  stores: Store[] = [];
  products: Product[] = [];
  reviews: DtoReview[] = [];

  selectedStoreId: number | null = null;
  selectedProductId: number | null = null;

  isLoadingStores = false;
  isLoadingProducts = false;
  isLoadingReviews = false;

  ngOnInit(): void {
    this.loadStores();
  }

  loadStores(): void {
    this.isLoadingStores = true;
    this.storeService.getMyStores().pipe(
      catchError(() => {
        this.toastService.showError('Failed to load stores.');
        this.isLoadingStores = false;
        return of([]);
      })
    ).subscribe(stores => {
      this.stores = stores || [];
      if (this.stores.length > 0 && this.stores[0].id) {
        this.selectedStoreId = this.stores[0].id;
        this.loadProducts();
      }
      this.isLoadingStores = false;
    });
  }

  onStoreChange(): void {
    this.products = [];
    this.reviews = [];
    this.selectedProductId = null;
    this.loadProducts();
  }

  loadProducts(): void {
    if (!this.selectedStoreId) return;
    this.isLoadingProducts = true;
    this.productService.getProductsByStoreId(this.selectedStoreId).pipe(
      catchError(() => {
        this.toastService.showError('Failed to load products.');
        this.isLoadingProducts = false;
        return of([]);
      })
    ).subscribe(products => {
      this.products = products || [];
      this.isLoadingProducts = false;
    });
  }

  onProductChange(): void {
    this.reviews = [];
    if (!this.selectedProductId) return;
    this.loadReviews();
  }

  loadReviews(): void {
    if (!this.selectedProductId) return;
    this.isLoadingReviews = true;
    this.reviewService.getByProductId(this.selectedProductId).pipe(
      catchError(() => {
        this.toastService.showError('Failed to load reviews.');
        this.isLoadingReviews = false;
        return of([]);
      })
    ).subscribe(reviews => {
      this.reviews = reviews || [];
      this.isLoadingReviews = false;
    });
  }

  getStars(rating: number): boolean[] {
    return Array.from({ length: 5 }, (_, i) => i < rating);
  }

  getSentimentClass(sentiment: Sentiment | undefined): string {
    switch (sentiment) {
      case Sentiment.POSITIVE: return 'sentiment-positive';
      case Sentiment.NEUTRAL:  return 'sentiment-neutral';
      case Sentiment.NEGATIVE: return 'sentiment-negative';
      default: return '';
    }
  }

  get averageRating(): number {
    if (!this.reviews.length) return 0;
    return this.reviews.reduce((sum, r) => sum + r.starRating, 0) / this.reviews.length;
  }

  get positiveCount(): number {
    return this.reviews.filter(r => r.sentiment === Sentiment.POSITIVE).length;
  }

  get negativeCount(): number {
    return this.reviews.filter(r => r.sentiment === Sentiment.NEGATIVE).length;
  }
}
