import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule, ReactiveFormsModule, FormBuilder, FormGroup, Validators } from '@angular/forms';
import { CategoryService } from '../../../core/services/category.service';
import { Category, CategoryRequest } from '../../../shared/models/category';

@Component({
  selector: 'app-admin-categories',
  standalone: true,
  imports: [CommonModule, FormsModule, ReactiveFormsModule],
  templateUrl: './admin-categories.component.html',
  styleUrl: './admin-categories.component.css'
})
export class AdminCategoriesComponent implements OnInit {

  categories: Category[] = [];
  isLoading = true;
  errorMessage = '';

  showCategoryModal = false;
  categoryForm: FormGroup;
  editingCategoryId: number | null = null;

  constructor(
    private categoryService: CategoryService,
    private fb: FormBuilder
  ) {
    this.categoryForm = this.fb.group({
      name: ['', Validators.required],
      parentId: [null]
    });
  }

  ngOnInit(): void {
    this.loadCategories();
  }

  loadCategories(): void {
    this.isLoading = true;
    this.categoryService.getAllCategories().subscribe({
      next: (data) => {
        this.categories = data || [];
        this.isLoading = false;
      },
      error: (err) => {
        console.error('Error loading categories', err);
        this.errorMessage = 'Failed to load categories.';
        this.isLoading = false;
      }
    });
  }

  openCategoryModal(category?: Category): void {
    if (category) {
      this.editingCategoryId = category.id;
      this.categoryForm.patchValue({
        name: category.name,
        parentId: category.parentId
      });
    } else {
      this.editingCategoryId = null;
      this.categoryForm.reset();
    }
    this.showCategoryModal = true;
  }

  closeCategoryModal(): void {
    this.showCategoryModal = false;
  }

  saveCategory(): void {
    if (this.categoryForm.invalid) return;

    const request: CategoryRequest = {
      name: this.categoryForm.value.name,
      parentId: this.categoryForm.value.parentId ? +this.categoryForm.value.parentId : null
    };

    if (this.editingCategoryId) {
      this.categoryService.updateCategoryById(this.editingCategoryId, request).subscribe({
        next: (updated) => {
          const idx = this.categories.findIndex(c => c.id === updated.id);
          if (idx !== -1) {
            this.categories[idx] = updated;
          } else {
            this.loadCategories();
          }
          this.closeCategoryModal();
        },
        error: (err) => {
          console.error('Error updating category', err);
          alert('Failed to update category.');
        }
      });
    } else {
      this.categoryService.createCategory(request).subscribe({
        next: (added) => {
          this.categories.push(added);
          this.closeCategoryModal();
        },
        error: (err) => {
          console.error('Error creating category', err);
          alert('Failed to create category.');
        }
      });
    }
  }

  deleteCategory(id: number): void {
    if (confirm('Are you sure you want to delete this category? Make sure no products depend on it!')) {
      this.categoryService.deleteCategoryById(id).subscribe({
        next: () => {
          this.categories = this.categories.filter(c => c.id !== id);
        },
        error: (err) => {
          console.error('Error deleting category', err);
          alert('Failed to delete category. It might be in use.');
        }
      });
    }
  }
}
