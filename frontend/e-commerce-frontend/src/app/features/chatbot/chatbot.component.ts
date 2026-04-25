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
                'Toplam sipariş sayım kaç?',
                'Bekleyen siparişlerimin toplam tutarı nedir?',
                'Tamamlanmış siparişlerimin sayısı',
                'En yüksek tutarlı 5 siparişim',
                'Mağazamdaki toplam ürün sayısı',
                'Stok miktarı 10\'dan az olan ürünlerim',
                'En pahalı 5 ürünüm hangileri?',
                'Kategorilere göre ürün dağılımım',
                'Ürünlerime verilen ortalama puan nedir?',
                'Pozitif yorum alan ürünlerim hangileri?',
                'Kargodaki siparişlerimin sayısı',
                'Depo bazında sevkiyat sayıları'
            ];
        } else if (this.userRole === 'INDIVIDUAL') {
            this.exampleQuestions = [
                'Siparişlerimin toplam tutarı nedir?',
                'Bekleyen siparişlerim var mı?',
                'Teslim edilen siparişlerimin sayısı',
                'En pahalı siparişim hangisi?',
                'İptal edilen siparişlerim',
                'Kaç ürüne yorum yaptım?',
                'Verdiğim yorumların ortalama puanı',
                'Kargoya verilen siparişlerim',
                'Son 10 siparişim'
            ];
        } else {
            this.exampleQuestions = [
                'Toplam kayıtlı kullanıcı sayısı',
                'Cinsiyete göre kullanıcı dağılımı',
                'Aktif mağaza sayısı kaç?',
                'En yeni 5 mağaza hangisi?',
                'Platformdaki toplam sipariş sayısı',
                'Duruma göre sipariş dağılımı',
                'En yüksek tutarlı 10 sipariş',
                'En çok stoğu olan 5 ürün',
                'Kategorilere göre ürün sayısı',
                'En pahalı 10 ürün hangileri?',
                'Ortalama ürün puanı nedir?',
                'Negatif yorum sayısı kaç?',
                'Kargo moduna göre sevkiyat dağılımı',
                'Depo bazında toplam sevkiyat sayısı'
            ];
        }
    }

    get hasMessages(): boolean { return this.messages.length > 0; }

    get headerSubtitle(): string {
        if (this.hasMessages) {
            const storeInfo = this.storeId ? `store_id: #${this.storeId} · ` : '';
            return `Aktif oturum · ${storeInfo}Guardrail: Açık`;
        }
        const storeInfo = this.storeId ? `— store_id: #${this.storeId}` : '';
        return `Mağaza verinize özel ${storeInfo}`;
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
                            detectionType: 'Nesne Enumeration (AV-09)',
                            trigger: `"${query.substring(0, 40)}"`,
                            target: 'ID tabanlı veri tarama',
                            action: 'Hesap 10 dakika engellendi',
                            badge: 'Güvenlik olayı loglandı · Rate Limit aktif'
                        },
                        timestamp: new Date()
                    });
                } else {
                    this.messages.push({
                        role: 'ai',
                        content: 'Bir hata oluştu. Lütfen tekrar deneyin.',
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
        if (gr) return { ...gr, timestamp: ts } as ChatMessage;

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
                    detectionType: 'Prompt Injection',
                    trigger:       this.extractInjectionTrigger(q),
                    target:        'store_id / rol kısıtlaması bypass',
                    action:        'İstek tamamen reddedildi',
                    blockedSql:    `SELECT * FROM orders -- WHERE store_id=? kaldırıldı (engellendi)`,
                    badge:         'Güvenlik olayı loglandı'
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
                    detectionType: 'SQL Injection',
                    trigger:       this.extractSqlTrigger(q),
                    target:        'Veritabanı bütünlüğü',
                    action:        'SQL üretimi durduruldu',
                    blockedSql:    `${q.substring(0, 60)} -- ENGELLENDİ`,
                    badge:         'Güvenlik olayı loglandı · db_executor blokladı'
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
                    detectionType: 'Yazma İşlemi / Mass Assignment',
                    trigger:       this.extractWriteTrigger(q),
                    target:        'Veritabanı kaydı değiştirme',
                    action:        'SELECT-only politikası aktif, yazma reddedildi',
                    blockedSql:    `UPDATE / INSERT -- ENGELLENDİ (SELECT-only)`,
                    badge:         'Güvenlik olayı loglandı'
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
                    detectionType: 'Hassas Veri Sızıntısı (AV-12)',
                    trigger:       this.extractExfilTrigger(q),
                    target:        'password_hash / api_key / internal_cost',
                    action:        'Kolon şeması kısıtlandı, istek reddedildi',
                    badge:         'Sütun beyaz listesi aktif · Güvenlik olayı loglandı'
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
                    detectionType: 'Sistem Prompt Sızıntısı (AV-07)',
                    trigger:       `"${q.substring(0, 40)}"`,
                    target:        'Sistem prompt / şema / konfigürasyon',
                    action:        'İntrospeksiyon isteği reddedildi',
                    badge:         'Güvenlik olayı loglandı'
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
                    detectionType: 'Yatay Yetki Yükseltme (AV-02)',
                    trigger:       storeMatch ? `store_id = ${storeMatch[1]}` : `"${q.substring(0, 40)}"`,
                    target:        storeMatch ? `Store #${storeMatch[1]} verisi` : 'Başka kullanıcı/mağaza verisi',
                    action:        'WHERE store_id kısıtlaması uygulandı, erişim reddedildi',
                    badge:         'Kapsam ihlali engellendi'
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
                    detectionType: 'Güvenlik Engeli',
                    trigger:       `"${q.substring(0, 40)}"`,
                    target:        'Platform güvenlik politikası',
                    action:        'İstek reddedildi',
                    badge:         'Güvenlik olayı loglandı'
                }
            };
        }

        return null;
    }

    // ── Guard helpers ─────────────────────────────────────────────────────────

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
            SCOPE:          'Guardrail Agent — KAPSAM DIŞI',
            ACCESS:         'Guardrail Agent — YETKİSİZ ERİŞİM',
            SECURITY:       'Guardrail Agent — GÜVENLİK ENGELİ',
            WRITE_ATTEMPT:  'Guardrail Agent — YAZMA DENEMESI',
            EXFILTRATION:   'Guardrail Agent — VERİ SIZINTISI',
            CONTEXT_POISON: 'Guardrail Agent — BAĞLAM ZEHİRLEME',
            RATE_LIMIT:     'Guardrail Agent — RATE LIMIT (AV-09)',
        };
        return type ? (map[type] ?? 'Guardrail Agent — GÜVENLİK ENGELİ') : 'Guardrail Agent';
    }

    guardrailHeaderText(type?: GuardrailType): string {
        if (type === 'SCOPE')        return 'Bu sorgu kısıtlı veri kapsamına giriyor.';
        if (type === 'RATE_LIMIT')   return 'Ardışık ID tarama girişimi tespit edildi.';
        if (type === 'ACCESS')       return 'Yetkisiz veri erişimi engellendi.';
        return 'Bu mesaj güvenlik filtrelerini tetikledi.';
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
        return this.aiService.formatMarkdown(content);
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
        const target   = event.target as HTMLElement;
        const expandBtn = target.closest('.expand-btn');
        if (expandBtn) {
            const wrapper = expandBtn.closest('.chat-img-wrapper');
            if (wrapper) {
                const img = wrapper.querySelector('img.chat-img') as HTMLImageElement;
                if (img?.src) this.expandGraph(img.src);
            }
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
