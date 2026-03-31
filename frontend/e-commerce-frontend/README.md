# 🛍️ E-Commerce Analytics Platform (Frontend)

Welcome to the frontend application for the E-Commerce Analytics Platform! This project is a modern, responsive, and robust web application built with **Angular**.
It serves as the main user interface for our e-commerce platform, providing a seamless experience for three distinct types of users: Individuals (Customers), Corporates (Store Owners), and Admins.

---

## ✨ Features and Capabilities

### 🔐 Secure Authentication & Authorization
- **Comprehensive Auth Flow:** Login, Registration, Forgot/Reset Password, and Email Verification.
- **Role-Based Access Control (RBAC):** Dedicated layouts, routes, and features based on user roles (`INDIVIDUAL`, `CORPORATE`, `ADMIN`) protected by auth and role route guards.

### 👤 Individual (Customer) Hub
*Designed for a smooth shopping experience.*
- **Product Discovery:** Browse products, view detailed descriptions, and manage shopping carts.
- **Order Management:** Place orders, track shipment history, and review past purchases.
- **Engagement:** Write and read product reviews.
- **Personal Analytics & Profile:** View personal shopping analytics and manage user profile details.

### 🏪 Corporate (Store Owner) Dashboard
*Empowering sellers to manage and grow their businesses.*
- **Store Analytics:** View comprehensive analytics dashboards to track store performance and sales.
- **Inventory & Order Management:** Manage product listings, track inventory levels, and process incoming customer orders.
- **Review Management:** Read and monitor customer reviews for their products.

### 🛡️ System Administration Panel
*The central control hub for platform administrators.*
- **Platform Overview:** High-level dashboard for monitoring system-wide activities.
- **Entity Management:** Oversee and manage all users, stores, and product categories globally.
- **System Configuration:** Keep track of global application settings and administrative controls.

---

## 💻 Tech Stack

- **Framework:** [Angular 19](https://angular.dev/)
- **Language:** [TypeScript](https://www.typescriptlang.org/)
- **Reactivity:** [RxJS](https://rxjs.dev/)
- **UI & Styling:** [Bootstrap 5](https://getbootstrap.com/) for rapid and responsive UI implementation.
- **Testing:** [Jasmine](https://jasmine.github.io/) & [Karma](https://karma-runner.github.io/) for dependable unit testing.

---

## 🚀 Getting Started

### Prerequisites
Make sure you have the following installed on your machine:
- [Node.js](https://nodejs.org/) (LTS recommended)
- [Angular CLI](https://github.com/angular/angular-cli) globally installed (`npm install -g @angular/cli`)

### Installation

1. Navigate into the frontend project directory:
   ```bash
   cd e-commerce-frontend
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Start the development server:
   ```bash
   ng serve
   ```

4. Open your browser and navigate to `http://localhost:4200/`. The application will automatically reload if you change any of the source files!

---

## 🛠️ Build and Test

- **Build for production:** Run `ng build` to build the required production files into the `dist/` directory.
- **Unit Testing:** Run `ng test` to execute the unit tests via Karma.
