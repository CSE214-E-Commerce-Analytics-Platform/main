import { Component, inject, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ReviewService } from '../../../core/services/review.service';
import { ToastService } from '../../../core/services/toast.service';
import { DtoReview, DtoReviewRequest, Sentiment } from '../../../shared/models/review';
import { catchError } from 'rxjs/operators';
import { of } from 'rxjs';

@Component({
  selector: 'app-ind-reviews',
  imports: [CommonModule, FormsModule],
  templateUrl: './ind-reviews.component.html',
  styleUrl: './ind-reviews.component.css'
})
export class IndReviewsComponent implements OnInit {
  private reviewService = inject(ReviewService);
  private toastService = inject(ToastService);

  Sentiment = Sentiment;

  reviews: DtoReview[] = [];
  isLoading = true;
  editingReviewId: number | null = null;
  editForm: DtoReviewRequest = { productId: 0, starRating: 0 };
  deletingId: number | null = null;
  savingId: number | null = null;

  ngOnInit(): void {
    this.loadReviews();
  }

  loadReviews(): void {
    this.isLoading = true;
    this.reviewService.getMyReviews().pipe(
      catchError(() => {
        this.toastService.showError('Failed to load reviews.');
        this.isLoading = false;
        return of([]);
      })
    ).subscribe(reviews => {
      this.reviews = reviews || [];
      this.isLoading = false;
    });
  }

  startEdit(review: DtoReview): void {
    this.editingReviewId = review.id!;
    this.editForm = { productId: review.productId, starRating: review.starRating, commentText: review.commentText };
  }

  cancelEdit(): void {
    this.editingReviewId = null;
  }

  saveEdit(reviewId: number): void {
    this.savingId = reviewId;
    this.reviewService.update(reviewId, this.editForm).pipe(
      catchError(err => {
        this.toastService.showError('Failed to update review. ' + (err.error?.exception?.message || ''));
        this.savingId = null;
        return of(null);
      })
    ).subscribe(updated => {
      if (updated) {
        this.toastService.showSuccess('Review updated.');
        this.editingReviewId = null;
        this.loadReviews();
      }
      this.savingId = null;
    });
  }

  deleteReview(reviewId: number): void {
    this.deletingId = reviewId;
    this.reviewService.deleteMyReview(reviewId).pipe(
      catchError(err => {
        this.toastService.showError('Failed to delete review. ' + (err.error?.exception?.message || ''));
        this.deletingId = null;
        return of(null);
      })
    ).subscribe(() => {
      this.toastService.showSuccess('Review deleted.');
      this.reviews = this.reviews.filter(r => r.id !== reviewId);
      this.deletingId = null;
    });
  }

  setEditStar(star: number): void {
    this.editForm.starRating = star;
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
}
