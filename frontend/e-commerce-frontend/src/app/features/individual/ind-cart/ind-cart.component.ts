import { Component, inject, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterLink } from '@angular/router';
import { CartService } from '../../../core/services/cart.service';
import { ToastService } from '../../../core/services/toast.service';

@Component({
  selector: 'app-ind-cart',
  imports: [CommonModule, RouterLink],
  templateUrl: './ind-cart.component.html',
  styleUrl: './ind-cart.component.css'
})
export class IndCartComponent implements OnInit {
  cartService = inject(CartService);
  toastService = inject(ToastService);

  showClearModal = false;

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

  checkout() {
    alert('Order placed successfully!');
    this.cartService.clearCart().subscribe();
  }

  getImageUrl(url: string | null | undefined): string {
    if (!url) return 'assets/placeholder-image.webp';
    if (url.startsWith('http://') || url.startsWith('https://')) return url;
    if (url.startsWith('assets/')) return url;
    return `assets/images/${url}`;
  }
}
