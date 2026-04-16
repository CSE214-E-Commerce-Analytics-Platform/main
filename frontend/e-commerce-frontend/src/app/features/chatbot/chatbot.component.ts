import { Component, inject, ElementRef, ViewChild, HostListener } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { AiAgentService } from '../../core/services/ai-agent.service';
import { AuthService } from '../../core/services/auth.service';

interface ChatMessage {
    role: 'user' | 'ai' | 'system';
    content: string;
    timestamp: Date;
}

@Component({
    selector: 'app-chatbot',
    imports: [CommonModule, FormsModule],
    templateUrl: './chatbot.component.html',
    styleUrl: './chatbot.component.css'
})
export class ChatbotComponent {
    @ViewChild('messagesContainer') messagesContainer!: ElementRef;

    private aiService = inject(AiAgentService);
    private authService = inject(AuthService);

    isOpen = false;
    userInput = '';
    messages: ChatMessage[] = [
        {
            role: 'ai',
            content: 'Hello! I\'m your AI product assistant. Ask me anything about your store\'s products — analytics, stock info, pricing, and more!',
            timestamp: new Date()
        }
    ];
    isTyping = false;
    isGraphExpanded = false;
    expandedGraphSrc = '';

    @HostListener('click', ['$event'])
    onClick(event: Event) {
        const target = event.target as HTMLElement;
        const expandBtn = target.closest('.expand-btn');
        if (expandBtn) {
            const wrapper = expandBtn.closest('.chat-img-wrapper');
            if (wrapper) {
                const img = wrapper.querySelector('img.chat-img') as HTMLImageElement;
                if (img && img.src) {
                    this.expandGraph(img.src);
                }
            }
        }
    }

    expandGraph(src: string): void {
        this.expandedGraphSrc = src;
        this.isGraphExpanded = true;
    }

    closeExpandedGraph(): void {
        this.isGraphExpanded = false;
        this.expandedGraphSrc = '';
    }

    toggleChat(): void {
        this.isOpen = !this.isOpen;
    }

    sendMessage(): void {
        const query = this.userInput.trim();
        if (!query || this.isTyping) return;

        // Validate for prompt injection
        const validationError = this.aiService.validateQuery(query);
        if (validationError) {
            this.messages.push({
                role: 'system',
                content: validationError,
                timestamp: new Date()
            });
            this.scrollToBottom();
            return;
        }

        // Add user message
        this.messages.push({
            role: 'user',
            content: query,
            timestamp: new Date()
        });
        this.userInput = '';
        this.isTyping = true;
        this.scrollToBottom();

        // 1. AuthService üzerinden kullanıcının rolünü alıyoruz.
        // NOT: Kendi AuthService altyapına göre burayı güncelle (Örn: authService.currentUserValue?.roleType)
        const userRole = this.authService.getRole() || 'INDIVIDUAL';

        // 2. Rol bilgisini aiService'e gönderiyoruz
        this.aiService.askQuestion(query, userRole).subscribe({
            next: (response: string) => {
                this.messages.push({
                    role: 'ai',
                    content: response,
                    timestamp: new Date()
                });
                this.isTyping = false;
                this.scrollToBottom();
            },
            error: (err: Error) => {
                this.messages.push({
                    role: 'system',
                    content: 'Sorry, I could not process your request. Please try again.',
                    timestamp: new Date()
                });
                this.isTyping = false;
                this.scrollToBottom();
            }
        });
    }

    onKeyDown(event: KeyboardEvent): void {
        if (event.key === 'Enter' && !event.shiftKey) {
            event.preventDefault();
            this.sendMessage();
            // Reset textarea height after sending
            const textarea = event.target as HTMLTextAreaElement;
            if (textarea) {
                textarea.style.height = 'auto';
            }
        }
    }

    autoResize(event: Event): void {
        const textarea = event.target as HTMLTextAreaElement;
        textarea.style.height = 'auto';
        textarea.style.height = textarea.scrollHeight + 'px';
    }

    formatContent(msg: ChatMessage): string {
        if (msg.role === 'user') {
            // User messages: just escape HTML
            return msg.content
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;');
        }

        // AI mesajını al
        let html = this.aiService.formatMarkdown(msg.content);

        // İçindeki resim (grafik) linklerini container, img ve expand butonuna çeviriyoruz
        html = html.replace(/!\[.*?\]\((.+?)\)/g, '<div class="chat-img-wrapper"><img src="$1" class="chat-img" alt="Graph"><div class="expand-btn" title="Expand Graph">⤢</div></div>');

        return html;
    }

    private scrollToBottom(): void {
        setTimeout(() => {
            if (this.messagesContainer) {
                const el = this.messagesContainer.nativeElement;
                el.scrollTop = el.scrollHeight;
            }
        }, 50);
    }
}