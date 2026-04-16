import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, map } from 'rxjs';
import { environment } from '../../../environments/environment.development';

@Injectable({
    providedIn: 'root'
})
export class AiAgentService {

    // Artık tek bir endpoint yok, rol tabanlı dinamik URL oluşturacağız
    private readonly baseAiUrl = `${environment.baseUrl}/ai/ask`;

    // Patterns that indicate prompt injection attempts
    private readonly INJECTION_PATTERNS = [
        /ignore\s+(all\s+)?(previous|prior|above)\s+(instructions|prompts|rules)/i,
        /system\s*:\s*/i,
        /you\s+are\s+now\s+(a|an)\s+/i,
        /forget\s+(all\s+)?(previous|your)\s+(instructions|rules|prompts)/i,
        /override\s+(system|safety|security)/i,
        /pretend\s+(you\s+are|to\s+be)/i,
        /act\s+as\s+(a|an|if)/i,
        /do\s+not\s+follow\s+(your|the)\s+(rules|instructions)/i,
        /reveal\s+(your|the|system)\s+(prompt|instructions|rules)/i,
        /what\s+(are|is)\s+your\s+(system|initial)\s+(prompt|instructions)/i,
        /disregard\s+(all|any|your)\s+(previous|prior|safety)/i,
        /jailbreak/i,
        /DAN\s+mode/i,
    ];

    private readonly MAX_QUERY_LENGTH = 500;

    constructor(private http: HttpClient) { }

    /**
     * Validates a user query for prompt injection attacks.
     * Returns null if safe, or an error message if suspicious.
     */
    validateQuery(query: string): string | null {
        if (!query || query.trim().length === 0) {
            return 'Please enter a question.';
        }

        if (query.length > this.MAX_QUERY_LENGTH) {
            return `Your question is too long. Maximum ${this.MAX_QUERY_LENGTH} characters allowed.`;
        }

        for (const pattern of this.INJECTION_PATTERNS) {
            if (pattern.test(query)) {
                return 'Your question contains restricted patterns. Please rephrase your question about products.';
            }
        }

        return null;
    }

    /**
     * Patterns that indicate a raw error or non-user-friendly response from the backend.
     */
    private readonly ERROR_RESPONSE_PATTERNS = [
        /\b\d{3}\s+(Service Unavailable|Internal Server Error|Bad Gateway|Not Found|Forbidden|Unauthorized|Bad Request|Gateway Timeout)\b/i,
        /\b(UNAVAILABLE|INTERNAL|DEADLINE_EXCEEDED|RESOURCE_EXHAUSTED|PERMISSION_DENIED)\b/,
        /\berror\b.*\bcode\b.*\bmessage\b/is,
        /\bstatus\b.*\b(UNAVAILABLE|ERROR|FAIL)\b/i,
        /"error"\s*:\s*\{/i,
        /\bException\b|\bStackTrace\b|\bat\s+[\w.]+\(.*:\d+\)/i,
        /\bPOST\s+request\s+for\b/i,
        /\bgenerativelanguage\.googleapis\.com\b/i,
        /\bThis model is currently experiencing high demand\b/i,
        /\bInformation regarding .* is unavailable in the provided dataset\b/i,
        /\bThe data only includes .* associated with\b/i,
    ];

    private readonly FALLBACK_MESSAGE =
        'Sorry, I wasn\'t able to get a proper answer right now. Please try again in a moment.';

    /**
     * Sends a validated query to the AI agent backend based on the user's role.
     * JSON body olarak gönderilir (@RequestBody AskAiRequest'e denk gelir).
     */
    askQuestion(question: string, role: string): Observable<string> {

        // 1. Role göre uygun endpoint suffix'ini belirle
        let endpointSuffix = '';
        if (role === 'INDIVIDUAL') {
            endpointSuffix = '/individual';
        } else if (role === 'CORPORATE') {
            endpointSuffix = '/corporate';
        } else if (role === 'ADMIN') {
            endpointSuffix = '/admin';
        } else {
            throw new Error('Geçersiz veya eksik kullanıcı rolü.');
        }

        const url = `${this.baseAiUrl}${endpointSuffix}`;

        // 2. HttpParams yerine JSON Body oluştur (Backend'deki AskAiRequest ile eşleşir)
        const body = { question: question };

        // 3. POST isteğini JSON gövdesiyle at
        return this.http.post(url, body, {
            responseType: 'text'
        }).pipe(
            map((raw: string) => {
                try {
                    const parsed = JSON.parse(raw);
                    const payload: string = parsed.payload ?? raw;
                    return this.sanitizeResponse(payload);
                } catch {
                    return this.sanitizeResponse(raw);
                }
            })
        );
    }

    /**
     * Checks if a response looks like a raw error or non-user-friendly message
     * and replaces it with a generic friendly response.
     */
    private sanitizeResponse(response: string): string {
        for (const pattern of this.ERROR_RESPONSE_PATTERNS) {
            if (pattern.test(response)) {
                return this.FALLBACK_MESSAGE;
            }
        }
        return response;
    }

    /**
     * Converts basic markdown syntax to HTML:
     */
    formatMarkdown(text: string): string {
        // Escape HTML special characters first
        let html = text
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');

        // Convert **bold** to <strong>
        html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');

        // Convert remaining *italic* to <em>
        html = html.replace(/(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)/g, '<em>$1</em>');

        // Process line-by-line for bullet lists
        const lines = html.split('\n');
        const result: string[] = [];
        let inList = false;

        for (const line of lines) {
            const trimmed = line.trim();
            if (trimmed.startsWith('• ') || trimmed.startsWith('- ') || /^\*\s/.test(trimmed)) {
                if (!inList) {
                    result.push('<ul>');
                    inList = true;
                }
                const content = trimmed.replace(/^[•\-\*]\s+/, '');
                result.push(`<li>${content}</li>`);
            } else {
                if (inList) {
                    result.push('</ul>');
                    inList = false;
                }
                if (trimmed === '') {
                    result.push('<br>');
                } else {
                    result.push(`<p>${trimmed}</p>`);
                }
            }
        }

        if (inList) {
            result.push('</ul>');
        }

        return result.join('');
    }
}