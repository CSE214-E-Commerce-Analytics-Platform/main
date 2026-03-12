import { Component, inject, ElementRef, ViewChild } from '@angular/core';
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

        // storeId auth yapısında mevcut değil, varsayılan değer kullanılıyor
        const storeId = 0;

        // Send to AI
        this.aiService.askQuestion(query, storeId).subscribe({
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
        return this.aiService.formatMarkdown(msg.content);
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
