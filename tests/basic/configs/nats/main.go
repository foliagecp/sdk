package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
)

func main() {
	// Порт для HTTPS сервера (не 443, т.к. там работает NATS WebSocket)
	port := os.Getenv("PORT")
	if port == "" {
		port = "8443" // По умолчанию используем 8443
	}

	// Пути к сертификатам (те же, что использует NATS)
	certFile := os.Getenv("CERT_FILE")
	keyFile := os.Getenv("KEY_FILE")
	natsWsHost := os.Getenv("NATS_WS_HOST")
	natsUser := os.Getenv("NATS_USER")
	natsPass := os.Getenv("NATS_PASS")

	if certFile == "" {
		certFile = "/etc/nats/server-cert.pem" // По умолчанию как в NATS конфиге
	}
	if keyFile == "" {
		keyFile = "/etc/nats/server-key.pem" // По умолчанию как в NATS конфиге
	}
	if natsWsHost == "" {
		natsWsHost = "localhost" // По умолчанию localhost
	}
	if natsUser == "" {
		natsUser = "nats" // По умолчанию как в NATS конфиге
	}
	if natsPass == "" {
		natsPass = "foliage" // По умолчанию как в NATS конфиге
	}

	// Главная страница для принятия сертификата
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		html := `
<!DOCTYPE html>
<html>
<head>
    <title>NATS TLS Test</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }
        .card { border: 1px solid #ddd; border-radius: 5px; padding: 20px; margin-bottom: 20px; }
        h1, h2 { color: #333; }
        code { background: #f5f5f5; padding: 2px 5px; border-radius: 3px; }
        .success { color: green; }
        .error { color: red; }
    </style>
</head>
<body>
    <h1>NATS TLS Certificate Accepted!</h1>
    
    <div class="card">
        <h2>Certificate Status</h2>
        <p class="success">✅ TLS certificate has been accepted by your browser</p>
        <p>Now you can connect to NATS WebSocket securely without certificate warnings.</p>
    </div>
    
    <div class="card">
        <h2>Connection Details</h2>
        <ul>
            <li>User: <code>` + natsUser + `</code></li>
            <li>Password: <code>` + natsPass + `</code></li>
        </ul>
    </div>
    
    <div class="card">
        <p><a href="/nats-test">Go to NATS WebSocket Test Page</a></p>
    </div>
</body>
</html>
`
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, html)
		log.Printf("Главная страница открыта: %s", r.RemoteAddr)
	})

	// Страница для тестирования подключения к NATS WebSocket
	http.HandleFunc("/nats-test", func(w http.ResponseWriter, r *http.Request) {
		html := `
<!DOCTYPE html>
<html>
<head>
    <title>NATS WebSocket Test</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 900px; margin: 0 auto; padding: 20px; }
        .card { border: 1px solid #ddd; border-radius: 5px; padding: 20px; margin-bottom: 20px; }
        h1, h2 { color: #333; }
        code { background: #f5f5f5; padding: 2px 5px; border-radius: 3px; font-family: monospace; }
        pre { background: #f5f5f5; padding: 10px; border-radius: 5px; overflow-x: auto; }
        button { background: #4CAF50; color: white; border: none; padding: 10px 15px; border-radius: 4px; cursor: pointer; }
        button:hover { background: #45a049; }
        button:disabled { background: #cccccc; }
        input, select { padding: 8px; width: 300px; }
        .form-group { margin-bottom: 15px; }
        label { display: block; margin-bottom: 5px; font-weight: bold; }
        #log { background: #f8f9fa; border: 1px solid #ddd; padding: 10px; height: 250px; overflow-y: auto; font-family: monospace; }
        .success { color: green; }
        .error { color: red; }
        .info { color: blue; }
    </style>
</head>
<body>
    <h1>NATS WebSocket Test Page</h1>
    
    <div class="card">
        <h2>Connection Settings</h2>
        <div class="form-group">
            <label for="server">NATS WebSocket URL:</label>
            <input type="text" id="server" value="ws://` + natsWsHost + `:443">
        </div>
        <div class="form-group">
            <label for="username">Username:</label>
            <input type="text" id="username" value="` + natsUser + `">
        </div>
        <div class="form-group">
            <label for="password">Password:</label>
            <input type="password" id="password" value="` + natsPass + `">
        </div>
        <button id="connect-btn">Connect</button>
        <button id="disconnect-btn" disabled>Disconnect</button>
    </div>
    
    <div class="card">
        <h2>Publish Message</h2>
        <div class="form-group">
            <label for="subject">Subject:</label>
            <input type="text" id="subject" value="test.message">
        </div>
        <div class="form-group">
            <label for="message">Message (JSON):</label>
            <input type="text" id="message" value='{"hello":"world"}'>
        </div>
        <button id="publish-btn" disabled>Publish</button>
    </div>
    
    <div class="card">
        <h2>Subscribe</h2>
        <div class="form-group">
            <label for="subscribe-subject">Subject:</label>
            <input type="text" id="subscribe-subject" value="test.*">
        </div>
        <button id="subscribe-btn" disabled>Subscribe</button>
        <button id="unsubscribe-btn" disabled>Unsubscribe</button>
    </div>
    
    <div class="card">
        <h2>Log</h2>
        <div id="log"></div>
    </div>

    <script type="module">
        import * as nats from 'https://cdn.jsdelivr.net/npm/nats.ws/esm/nats.js';
        
        // UI элементы
        const connectBtn = document.getElementById('connect-btn');
        const disconnectBtn = document.getElementById('disconnect-btn');
        const publishBtn = document.getElementById('publish-btn');
        const subscribeBtn = document.getElementById('subscribe-btn');
        const unsubscribeBtn = document.getElementById('unsubscribe-btn');
        const logEl = document.getElementById('log');
        
        // Состояние
        let nc = null;
        let subscription = null;
        
        // Логгирование
        function log(message, type = 'info') {
            const el = document.createElement('div');
            el.textContent = new Date().toLocaleTimeString() + ' - ' + message;
            el.className = type;
            logEl.appendChild(el);
            logEl.scrollTop = logEl.scrollHeight;
        }
        
        // Подключение к NATS
        connectBtn.addEventListener('click', async () => {
            const serverUrl = document.getElementById('server').value;
            const username = document.getElementById('username').value;
            const password = document.getElementById('password').value;
            
            log('Connecting to ' + serverUrl + '...');
            
            try {
                nc = await nats.connect({
                    servers: serverUrl,
                    user: username,
                    pass: password
                });
                
                log('Connected successfully!', 'success');
                
                // Обновление UI
                connectBtn.disabled = true;
                disconnectBtn.disabled = false;
                publishBtn.disabled = false;
                subscribeBtn.disabled = false;
                
                // Мониторинг закрытия соединения
                (async () => {
                    await nc.closed();
                    log('Connection closed', 'info');
                    resetUI();
                })().catch(err => {
                    log('Connection error: ' + err.message, 'error');
                    resetUI();
                });
                
            } catch (err) {
                log('Connection failed: ' + err.message, 'error');
                console.error('Connection error:', err);
            }
        });
        
        // Отключение
        disconnectBtn.addEventListener('click', async () => {
            if (nc) {
                log('Disconnecting...');
                try {
                    await nc.close();
                    log('Disconnected', 'success');
                    resetUI();
                } catch (err) {
                    log('Error disconnecting: ' + err.message, 'error');
                }
            }
        });
        
        // Публикация сообщения
        publishBtn.addEventListener('click', async () => {
            if (!nc) {
                log('Not connected', 'error');
                return;
            }
            
            const subject = document.getElementById('subject').value;
            const message = document.getElementById('message').value;
            
            try {
                log('Publishing to ' + subject + ': ' + message);
                await nc.publish(subject, nats.StringCodec().encode(message));
                log('Published successfully', 'success');
            } catch (err) {
                log('Publish error: ' + err.message, 'error');
            }
        });
        
        // Подписка
        subscribeBtn.addEventListener('click', async () => {
            if (!nc) {
                log('Not connected', 'error');
                return;
            }
            
            if (subscription) {
                log('Already subscribed. Unsubscribe first.', 'error');
                return;
            }
            
            const subject = document.getElementById('subscribe-subject').value;
            
            try {
                log('Subscribing to ' + subject);
                subscription = nc.subscribe(subject);
                
                subscribeBtn.disabled = true;
                unsubscribeBtn.disabled = false;
                
                // Обработка входящих сообщений
                (async () => {
                    for await (const msg of subscription) {
                        const data = nats.StringCodec().decode(msg.data);
                        log('Received message on ' + msg.subject + ': ' + data, 'success');
                    }
                    log('Subscription ended', 'info');
                })().catch(err => {
                    log('Subscription error: ' + err.message, 'error');
                });
                
                log('Subscribed successfully', 'success');
            } catch (err) {
                log('Subscribe error: ' + err.message, 'error');
            }
        });
        
        // Отписка
        unsubscribeBtn.addEventListener('click', async () => {
            if (subscription) {
                try {
                    log('Unsubscribing...');
                    await subscription.unsubscribe();
                    subscription = null;
                    
                    subscribeBtn.disabled = false;
                    unsubscribeBtn.disabled = true;
                    
                    log('Unsubscribed successfully', 'success');
                } catch (err) {
                    log('Unsubscribe error: ' + err.message, 'error');
                }
            }
        });
        
        // Сброс UI
        function resetUI() {
            connectBtn.disabled = false;
            disconnectBtn.disabled = true;
            publishBtn.disabled = true;
            subscribeBtn.disabled = true;
            unsubscribeBtn.disabled = true;
            nc = null;
            subscription = null;
        }
        
        // Проверяем библиотеку
        log('NATS WebSocket client initialized');
        try {
            log('NATS library loaded: ' + nats.VERSION);
        } catch (err) {
            log('Error loading NATS library', 'error');
        }
    </script>
</body>
</html>
`
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, html)
		log.Printf("Страница тестирования открыта: %s", r.RemoteAddr)
	})

	log.Printf("Запуск HTTPS сервера на порту %s...", port)
	log.Printf("Для принятия сертификата откройте: https://localhost:%s/", port)
	log.Printf("Для тестирования NATS WebSocket: https://localhost:%s/nats-test", port)
	log.Printf("NATS WebSocket настроен на: wss://%s:443", natsWsHost)

	err := http.ListenAndServeTLS(":"+port, certFile, keyFile, nil)
	if err != nil {
		log.Fatalf("Ошибка запуска HTTPS сервера: %v", err)
	}
}
