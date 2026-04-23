export enum Sentiment {
  POSITIVE = 'POSITIVE',
  NEUTRAL = 'NEUTRAL',
  NEGATIVE = 'NEGATIVE'
}

export interface DtoReview {
  id?: number;
  productId: number;
  productName?: string;
  userId?: number;
  starRating: number;
  commentText?: string;
  sentiment?: Sentiment;
}

export interface DtoReviewRequest {
  productId: number;
  starRating: number;
  commentText?: string;
}
