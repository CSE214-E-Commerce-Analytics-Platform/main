import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { CartService } from '../../../core/services/cart.service';
import { DtoCart } from '../../../shared/models/cart';

@Component({
  selector: 'app-admin-carts',
  imports: [CommonModule],
  templateUrl: './admin-carts.component.html',
  styleUrl: './admin-carts.component.css'
})
export class AdminCartsComponent implements OnInit {
  cartService = inject(CartService);

  carts: DtoCart[] = [];
  isLoading = true;
  errorMessage = '';

  ngOnInit(): void {
    this.fetchCarts();
  }

  fetchCarts(): void {
    this.isLoading = true;
    this.cartService.findAllCarts().subscribe({
      next: (res) => {
        if (res.payload) {
          this.carts = res.payload;
        }
        this.isLoading = false;
      },
      error: (err) => {
        console.error(err);
        this.errorMessage = 'Failed to load carts.';
        this.isLoading = false;
      }
    });
  }

  deleteCart(cartId: number): void {
    if (confirm(`Are you sure you want to delete cart ID: ${cartId}?`)) {
      this.cartService.adminDeleteCart(cartId).subscribe({
        next: () => {
          this.carts = this.carts.filter(c => c.id !== cartId);
        },
        error: (err) => {
          console.error(err);
          alert('Failed to delete cart.');
        }
      });
    }
  }
}
