import { Component, inject, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterLink } from '@angular/router';
import { CartService } from '../../../core/services/cart.service';
import { ToastService } from '../../../core/services/toast.service';
import { FormsModule } from '@angular/forms';
import { OrderService } from '../../../core/services/order.service';
import { PaymentService } from '../../../core/services/payment.service';
import { catchError } from 'rxjs/operators';
import { of } from 'rxjs';
import { environment } from '../../../../environments/environment.development';

declare var Stripe: any;

@Component({
  selector: 'app-ind-cart',
  imports: [CommonModule, RouterLink, FormsModule],
  templateUrl: './ind-cart.component.html',
  styleUrl: './ind-cart.component.css'
})
export class IndCartComponent implements OnInit {
  cartService = inject(CartService);
  toastService = inject(ToastService);
  orderService = inject(OrderService);
  paymentService = inject(PaymentService);

  showClearModal = false;
  showCheckoutModal = false;
  shippingAddress = '';
  isProcessing = false;

  ngOnInit(): void {
    // Current cart state flows from Header via the BehaviorSubject,
    // but good practice to ensure it's fresh on page load
    this.cartService.refreshMyCart().subscribe();
  }

  updateQuantity(itemId: number, newQuantity: number) {
    if (newQuantity < 1) return;
    this.cartService.updateQuantity(itemId, newQuantity).subscribe();
  }

  removeItem(itemId: number) {
    this.cartService.removeItem(itemId).subscribe();
  }

  confirmClearCart() {
    this.showClearModal = true;
  }

  cancelClearCart() {
    this.showClearModal = false;
  }

  executeClearCart() {
    this.cartService.clearCart().subscribe({
      next: () => {
        this.showClearModal = false;
        this.toastService.showInfo('Cart cleared successfully.');
      }
    });
  }

  openCheckoutModal() {
    this.showCheckoutModal = true;
  }

  cancelCheckout() {
    this.showCheckoutModal = false;
    this.shippingAddress = '';
  }

  proceedToPayment() {
    if (!this.shippingAddress.trim()) {
      this.toastService.showError('Please enter a shipping address.');
      return;
    }

    this.isProcessing = true;

    // 1. Create the order
    this.orderService.create({
      shippingAddress: this.shippingAddress,
      shippingCost: 0 // Optional logic for shipping cost
    }).pipe(
      catchError(err => {
        this.toastService.showError('Failed to create order. ' + (err.error?.exception?.message || ''));
        this.isProcessing = false;
        return of(null);
      })
    ).subscribe(createdOrder => {
      if (createdOrder && createdOrder.id) {
        // 2. Initiate Payment
        this.paymentService.create({
          orderId: createdOrder.id
        }).pipe(
          catchError(err => {
            this.toastService.showError('Failed to initiate payment. ' + (err.error?.exception?.message || ''));
            this.isProcessing = false;
            return of(null);
          })
        ).subscribe(payment => {
          if (payment && payment.transactionKey) {
            this.toastService.showSuccess('Redirecting to payment gateway...');
            const stripe = Stripe(environment.stripePublicKey);
            stripe.redirectToCheckout({ sessionId: payment.transactionKey }).then((result: any) => {
              if (result.error) {
                this.toastService.showError(result.error.message);
                this.isProcessing = false;
              }
            });
          } else {
            this.isProcessing = false;
          }
        });
      }
    });
  }

  getImageUrl(url: string | null | undefined): string {
    if (!url) return 'assets/placeholder-image.webp';
    if (url.startsWith('http://') || url.startsWith('https://')) return url;
    if (url.startsWith('assets/')) return url;
    return `assets/images/${url}`;
  }
}
