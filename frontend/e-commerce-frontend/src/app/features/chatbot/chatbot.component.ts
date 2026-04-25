import { Component, inject, ElementRef, ViewChild, HostListener, OnInit } from '@angular/core';
import { CommonModule, DecimalPipe } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { AiAgentService, AiResponse } from '../../core/services/ai-agent.service';
import { AuthService } from '../../core/services/auth.service';

type GuardrailType = 'INJECTION' | 'SCOPE' | 'ACCESS' | 'SECURITY' | 'SQL_INJECTION' | 'RATE_LIMIT' | 'WRITE_ATTEMPT' | 'EXFILTRATION' | 'CONTEXT_POISON';

interface GuardrailDetail {
    detectionType:  string;   // e.g. "Prompt Injection"
    trigger:        string;   // the suspicious phrase
    target:         string;   // what the attacker was trying to reach
    action:         string;   // what the system did
    blockedSql?:    string;   // the SQL that would have run (crossed out)
    suggestion?:    string;   // alternative action offered to the user
    badge?:         string;   // bottom badge label
}

interface BarItem { label: string; value: number; unit: string; pct: number; }

interface ChatMessage {
    role: 'user' | 'ai' | 'guardrail';
    content: string;
    // AI fields
    sqlQuery?:     string | null;
    barTitle?:     string;
    barItems?:     BarItem[];
    rowCount?:     number;
    queryTimeMs?:  number;
    trace?:        string[];
    suggestions?:  string[];
    // Guardrail fields
    guardrailType?:    GuardrailType;
    guardrailDetail?:  GuardrailDetail;
    timestamp: Date;
}

@Component({
    selector: 'app-chatbot',
    standalone: true,
    imports: [CommonModule, FormsModule, DecimalPipe],
    templateUrl: './chatbot.component.html',
    styleUrl: './chatbot.component.css'
})
export class ChatbotComponent implements OnInit {
    @ViewChild('messagesContainer') messagesContainer!: ElementRef;

    private aiService  = inject(AiAgentService);
    private authService = inject(AuthService);

    isOpen       = false;
    userInput    = '';
    isTyping     = false;
    userRole     = '';
    storeId: number | null = null;
    displayName  = '';
    messages: ChatMessage[] = [];
    exampleQuestions: string[] = [];

    isGraphExpanded = false;
    expandedGraphSrc = '';

    ngOnInit(): void {
        this.userRole    = this.authService.getRole() || 'INDIVIDUAL';
        this.storeId     = this.getStoreId();
        const email      = this.authService.getCurrentUserEmail() || '';
        this.displayName = email.split('@')[0] || 'Kullanıcı';
        this.setExampleQuestions();
    }

    private getStoreId(): number | null {
        const token = this.authService.getAccessToken();
        if (!token) return null;
        try {
            const payload = JSON.parse(atob(token.split('.')[1]));
            return payload.storeId ?? payload.store_id ?? null;
        } catch { return null; }
    }

    private setExampleQuestions(): void {
        if (this.userRole === 'CORPORATE') {
            this.exampleQuestions = [
                'What is my total order count?',
                'What is the total amount of my pending orders?',
                'How many completed orders do I have?',
                'Show my top 5 highest value orders',
                'Total number of products in my store',
                'Products with less than 10 stock',
                'What are my 5 most expensive products?',
                'Product distribution by categories',
                'What is the average rating given to my products?',
                'Which of my products received positive reviews?',
                'How many of my orders are shipped?',
                'Number of shipments by warehouse'
            ];
        } else if (this.userRole === 'INDIVIDUAL') {
            this.exampleQuestions = [
                'What is the total amount of my orders?',
                'Do I have any pending orders?',
                'Number of my delivered orders',
                'Which is my most expensive order?',
                'My canceled orders',
                'How many products did I review?',
                'Average rating of my reviews',
                'My shipped orders',
                'My last 10 orders'
            ];
        } else {
            this.exampleQuestions = [
                'Total registered user count',
                'User distribution by gender',
                'How many active stores are there?',
                'Which are the 5 newest stores?',
                'Total number of orders on the platform',
                'Order distribution by status',
                'Top 10 highest value orders',
                'Top 5 products with the most stock',
                'Number of products by category',
                'What are the 10 most expensive products?',
                'What is the average product rating?',
                'How many negative reviews are there?',
                'Shipment distribution by mode',
                'Total shipment count by warehouse'
            ];
        }
    }

    get hasMessages(): boolean { return this.messages.length > 0; }

    get headerSubtitle(): string {
        if (this.hasMessages) {
            const storeInfo = this.storeId ? `store_id: #${this.storeId} · ` : '';
            return `Active session · ${storeInfo}Guardrail: Active`;
        }
        const storeInfo = this.storeId ? `— store_id: #${this.storeId}` : '';
        return `Custom to your store data ${storeInfo}`;
    }

    toggleChat(): void { this.isOpen = !this.isOpen; }
    closeChat():  void { this.isOpen = false; }

    useExample(q: string): void {
        this.userInput = q;
        this.sendMessage();
    }

    sendMessage(): void {
        const query = this.userInput.trim();
        if (!query || this.isTyping) return;

        this.messages.push({ role: 'user', content: query, timestamp: new Date() });
        this.userInput = '';
        this.isTyping  = true;
        this.scrollToBottom();

        const startTime = Date.now();

        this.aiService.askQuestion(query, this.userRole).subscribe({
            next: (res: AiResponse) => {
                const queryTimeMs = Date.now() - startTime;
                this.messages.push(this.buildMessage(res, query, queryTimeMs));
                this.isTyping = false;
                this.scrollToBottom();
            },
            error: (err) => {
                // HTTP 429 — rate limit block
                if (err?.status === 429) {
                    this.messages.push({
                        role: 'guardrail',
                        content: '',
                        guardrailType: 'RATE_LIMIT',
                        guardrailDetail: {
                            detectionType: 'Object Enumeration (AV-09)',
                            trigger: `"${query.substring(0, 40)}"`,
                            target: 'ID-based data scraping',
                            action: 'Account blocked for 10 minutes',
                            badge: 'Security event logged · Rate Limit active'
                        },
                        timestamp: new Date()
                    });
                } else {
                    this.messages.push({
                        role: 'ai',
                        content: 'An error occurred. Please try again.',
                        timestamp: new Date()
                    });
                }
                this.isTyping = false;
                this.scrollToBottom();
            }
        });
    }

    // ── Map server response → ChatMessage ──────────────────────────────────────

    private buildMessage(res: AiResponse, userQuestion: string, queryTimeMs: number): ChatMessage {
        const ts     = new Date();
        const answer = res.answer;

        // ── Detect guardrail blocks from answer text ──────────────────────────
        const gr = this.detectGuardrail(answer, userQuestion, res);
        if (gr) return { ...gr, timestamp: ts, trace: res.trace } as ChatMessage;

        // ── Normal AI response ────────────────────────────────────────────────
        const hasChartImage = answer.includes('![');
        const barItems      = hasChartImage ? [] : this.parseBarItems(answer);
        const hasChart      = barItems.length >= 2;

        return {
            role:      'ai',
            content:   answer,
            sqlQuery:  res.sqlQuery,
            barTitle:  hasChart ? this.extractBarTitle(answer)  : undefined,
            barItems:  hasChart ? barItems                       : undefined,
            rowCount:  hasChart ? barItems.length                : undefined,
            queryTimeMs,
            trace:     res.trace,
            suggestions: res.suggestions,
            timestamp: ts
        };
    }

    // ── Guardrail detection — maps response text to rich UI card ─────────────

    private detectGuardrail(answer: string, q: string, res: AiResponse): Partial<ChatMessage> | null {
        const lower  = answer.toLowerCase();
        const qLower = q.toLowerCase();

        // ── AV-01 / AV-10: Prompt Injection / Context Poisoning ──────────────
        if (this.isUnsafeBlock(lower) && this.hasInjectionTokens(qLower)) {
            return {
                role: 'guardrail',
                content: answer,
                guardrailType: 'INJECTION',
                guardrailDetail: {
                    detectionType: 'Prompt Injection (AV-01)',
                    trigger:       this.extractInjectionTrigger(q),
                    target:        'store_id / role constraint bypass',
                    action:        'Request entirely rejected',
                    blockedSql:    `SELECT * FROM orders -- WHERE store_id=? removed (blocked)`,
                    badge:         'Security event logged'
                }
            };
        }

        // ── AV-03: SQL Injection ─────────────────────────────────────────────
        if (this.isUnsafeBlock(lower) && this.hasSqlInjectionTokens(qLower)) {
            return {
                role: 'guardrail',
                content: answer,
                guardrailType: 'SQL_INJECTION',
                guardrailDetail: {
                    detectionType: 'SQL Injection (AV-03)',
                    trigger:       this.extractSqlTrigger(q),
                    target:        'Database integrity',
                    action:        'SQL generation stopped',
                    blockedSql:    `${q.substring(0, 60)} -- BLOCKED`,
                    badge:         'Security event logged · Blocked by db_executor'
                }
            };
        }

        // ── AV-11: Write / Mass Assignment ───────────────────────────────────
        if (this.isUnsafeBlock(lower) && this.hasWriteTokens(qLower)) {
            return {
                role: 'guardrail',
                content: answer,
                guardrailType: 'WRITE_ATTEMPT',
                guardrailDetail: {
                    detectionType: 'Write Attempt / Mass Assignment (AV-11)',
                    trigger:       this.extractWriteTrigger(q),
                    target:        'Database record modification',
                    action:        'SELECT-only policy active, write rejected',
                    blockedSql:    `UPDATE / INSERT -- BLOCKED (SELECT-only)`,
                    badge:         'Security event logged'
                }
            };
        }

        // ── AV-12: Exfiltration ──────────────────────────────────────────────
        if (this.isUnsafeBlock(lower) && this.hasExfilTokens(qLower)) {
            return {
                role: 'guardrail',
                content: answer,
                guardrailType: 'EXFILTRATION',
                guardrailDetail: {
                    detectionType: 'Sensitive Data Leak (AV-12)',
                    trigger:       this.extractExfilTrigger(q),
                    target:        'password_hash / api_key / internal_cost',
                    action:        'Column schema restricted, request rejected',
                    badge:         'Column whitelist active · Security event logged'
                }
            };
        }

        // ── AV-07: Prompt Leakage ────────────────────────────────────────────
        if (this.isUnsafeBlock(lower) && this.hasLeakageTokens(qLower)) {
            return {
                role: 'guardrail',
                content: answer,
                guardrailType: 'SECURITY',
                guardrailDetail: {
                    detectionType: 'System Prompt Leakage (AV-07)',
                    trigger:       `"${q.substring(0, 40)}"`,
                    target:        'System prompt / schema / configuration',
                    action:        'Introspection request rejected',
                    badge:         'Security event logged'
                }
            };
        }

        // ── AV-02: Cross-scope / authorization ──────────────────────────────
        if (lower.includes('permission') || lower.includes('yetkisiz') || lower.includes('do not have permission')) {
            const storeMatch = q.match(/store[_\s#]*(\d+)/i) || q.match(/#(\d+)/);
            return {
                role: 'guardrail',
                content: answer,
                guardrailType: 'ACCESS',
                guardrailDetail: {
                    detectionType: 'Horizontal Privilege Escalation (AV-02)',
                    trigger:       storeMatch ? `store_id = ${storeMatch[1]}` : `"${q.substring(0, 40)}"`,
                    target:        storeMatch ? `Store #${storeMatch[1]} data` : 'Other user/store data',
                    action:        'WHERE store_id restriction applied, access denied',
                    badge:         'Scope violation blocked'
                }
            };
        }

        // ── Generic unsafe block ─────────────────────────────────────────────
        if (this.isUnsafeBlock(lower)) {
            return {
                role: 'guardrail',
                content: answer,
                guardrailType: 'SECURITY',
                guardrailDetail: {
                    detectionType: 'Security Block',
                    trigger:       `"${q.substring(0, 40)}"`,
                    target:        'Platform security policy',
                    action:        'Request rejected',
                    badge:         'Security event logged'
                }
            };
        }

        return null;
    }

    askSuggestion(suggestion: string): void {
        this.userInput = suggestion;
        this.sendMessage();
    }

    // ── Utils ─────────────────────────────────────────────────────────────────

    private isUnsafeBlock(lower: string): boolean {
        return lower.includes("unable to help") ||
               lower.includes("cannot answer")  ||
               lower.includes("cannot help")    ||
               lower.includes("[security]");
    }

    private hasInjectionTokens(q: string): boolean {
        return /ignore.*(previous|instructions?|prompt)|system\s+override|you are now|act as|jailbreak|dan mode|for testing.*admin|context.*system|elevated to admin/i.test(q);
    }

    private hasSqlInjectionTokens(q: string): boolean {
        return /drop\s+table|union\s+select|insert\s+into|--\s*$|;\s*(select|drop|insert|update|delete)|where\s+1\s*=\s*1/i.test(q);
    }

    private hasWriteTokens(q: string): boolean {
        return /\b(update|delete|drop|insert|set\s+my\s+role|set\s+role|add.*admin|create.*user)\b/i.test(q);
    }

    private hasExfilTokens(q: string): boolean {
        return /password_hash|api_key|internal.*cost|supplier.*margin|all.*internal|everything.*account/i.test(q);
    }

    private hasLeakageTokens(q: string): boolean {
        return /system\s+prompt|your\s+instructions?|initialization|repeat.*verbatim|print.*above|raw.*context|what\s+tables\s+exist/i.test(q);
    }

    // ── Trigger extractors ────────────────────────────────────────────────────

    private extractInjectionTrigger(q: string): string {
        const m = q.match(/ignore\s+(?:previous|your|all)\s+instructions?/i) ||
                  q.match(/\[(?:system|context)[^\]]*\]/i)                    ||
                  q.match(/you\s+are\s+now/i)                                 ||
                  q.match(/elevated\s+to\s+admin/i);
        return m ? `"${m[0]}"` : `"${q.substring(0, 45)}..."`;
    }

    private extractSqlTrigger(q: string): string {
        const m = q.match(/drop\s+table\s+\w+/i)  ||
                  q.match(/union\s+select[\w\s,]+/i)||
                  q.match(/insert\s+into\s+\w+/i)  ||
                  q.match(/where\s+1\s*=\s*1/i);
        return m ? `"${m[0]}"` : `"${q.substring(0, 45)}..."`;
    }

    private extractWriteTrigger(q: string): string {
        const m = q.match(/set\s+(?:my\s+)?role\s+to\s+\w+/i) ||
                  q.match(/add\s+.*admin/i)                     ||
                  q.match(/update\s+\w+\s+to/i);
        return m ? `"${m[0]}"` : `"${q.substring(0, 45)}..."`;
    }

    private extractExfilTrigger(q: string): string {
        const m = q.match(/password_hash|api_key|internal_cost|supplier_margin/i);
        return m ? `"${m[0]}"` : `"${q.substring(0, 45)}..."`;
    }

    // ── Bar chart helpers ─────────────────────────────────────────────────────

    private parseBarItems(text: string): BarItem[] {
        const re = /^(?:\d+\.\s+)?(.+?)[:：]\s*([\d,.]+)\s*(ad\.|adet|units?|₺|TL|%|kg)?/;
        const items: BarItem[] = [];
        for (const line of text.split('\n')) {
            const m = line.trim().match(re);
            if (m) {
                const value = parseFloat(m[2].replace(',', ''));
                if (!isNaN(value) && value > 0)
                    items.push({ label: m[1].trim(), value, unit: m[3]?.trim() || '', pct: 0 });
            }
        }
        if (items.length >= 2) {
            const max = Math.max(...items.map(i => i.value));
            return items.map(i => ({ ...i, pct: Math.round((i.value / max) * 100) }));
        }
        return [];
    }

    private extractBarTitle(text: string): string {
        for (const line of text.split('\n')) {
            const t = line.trim();
            if (!t) continue;
            if (t.match(/^(?:\d+\.\s|[-•]\s)/)) break;
            if (!t.match(/:\s*[\d,]+\s*(?:ad\.|adet|units?)?$/)) return t;
        }
        return '';
    }

    // ── Guardrail label helpers ────────────────────────────────────────────────

    guardrailLabel(type?: GuardrailType): string {
        const map: Record<GuardrailType, string> = {
            INJECTION:      'Guardrail Agent — PROMPT INJECTION',
            SQL_INJECTION:  'Guardrail Agent — SQL INJECTION',
            SCOPE:          'Guardrail Agent — OUT OF SCOPE',
            ACCESS:         'Guardrail Agent — UNAUTHORIZED ACCESS',
            SECURITY:       'Guardrail Agent — SECURITY BLOCK',
            WRITE_ATTEMPT:  'Guardrail Agent — WRITE ATTEMPT',
            EXFILTRATION:   'Guardrail Agent — DATA LEAK',
            CONTEXT_POISON: 'Guardrail Agent — CONTEXT POISONING',
            RATE_LIMIT:     'Guardrail Agent — RATE LIMIT (AV-09)',
        };
        return type ? (map[type] ?? 'Guardrail Agent — SECURITY BLOCK') : 'Guardrail Agent';
    }

    guardrailHeaderText(type?: GuardrailType): string {
        if (type === 'SCOPE')        return 'This query falls out of the restricted data scope.';
        if (type === 'RATE_LIMIT')   return 'Sequential ID scraping attempt detected.';
        if (type === 'ACCESS')       return 'Unauthorized data access blocked.';
        return 'This message triggered security filters.';
    }

    guardrailBadgeClass(type?: GuardrailType): string {
        if (!type) return 'gr-badge-label injection';
        if (['INJECTION','SQL_INJECTION','WRITE_ATTEMPT','CONTEXT_POISON'].includes(type)) return 'gr-badge-label injection';
        if (['SCOPE','ACCESS','EXFILTRATION'].includes(type)) return 'gr-badge-label scope';
        if (type === 'RATE_LIMIT') return 'gr-badge-label rate';
        return 'gr-badge-label security';
    }

    // ── Misc helpers ──────────────────────────────────────────────────────────

    formatAiContent(content: string): string {
        let html = this.aiService.formatMarkdown(content);
        // <img> tag'lerini küçük thumbnail + expand butonuna çevir
        html = html.replace(
            /<img\s+src="([^"]+)"[^>]*>/g,
            '<div class="chat-img-wrapper"><img src="$1" class="chat-img" alt="Graph"><div class="expand-btn" title="Büyüt">⤢</div></div>'
        );
        return html;
    }

    formatQueryTime(ms: number): string {
        return ms >= 1000 ? `${(ms / 1000).toFixed(2)}s` : `${ms}ms`;
    }

    onKeyDown(event: KeyboardEvent): void {
        if (event.key === 'Enter' && !event.shiftKey) {
            event.preventDefault();
            this.sendMessage();
            const ta = event.target as HTMLTextAreaElement;
            if (ta) ta.style.height = 'auto';
        }
    }

    autoResize(event: Event): void {
        const ta = event.target as HTMLTextAreaElement;
        ta.style.height = 'auto';
        ta.style.height = Math.min(ta.scrollHeight, 100) + 'px';
    }

    @HostListener('click', ['$event'])
    onClick(event: Event) {
        const target = event.target as HTMLElement;

        // Expand button click
        const expandBtn = target.closest('.expand-btn');
        if (expandBtn) {
            const wrapper = expandBtn.closest('.chat-img-wrapper');
            if (wrapper) {
                const img = wrapper.querySelector('img.chat-img') as HTMLImageElement;
                if (img?.src) this.expandGraph(img.src);
            }
            return;
        }

        // Resmin kendisine tıklayınca da expand açılır
        if (target.classList.contains('chat-img')) {
            const img = target as HTMLImageElement;
            if (img.src) this.expandGraph(img.src);
        }
    }

    expandGraph(src: string): void { this.expandedGraphSrc = src; this.isGraphExpanded = true; }
    closeExpandedGraph(): void     { this.isGraphExpanded = false; this.expandedGraphSrc = ''; }

    private scrollToBottom(): void {
        setTimeout(() => {
            if (this.messagesContainer) {
                const el = this.messagesContainer.nativeElement;
                el.scrollTop = el.scrollHeight;
            }
        }, 50);
    }
}
