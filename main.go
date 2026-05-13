package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// --- Domain Models ---

type RiskLevel string

const (
	RiskLow      RiskLevel = "low"
	RiskMedium   RiskLevel = "medium"
	RiskHigh     RiskLevel = "high"
	RiskCritical RiskLevel = "critical"
)

type Transaction struct {
	ID         string    `json:"id"`
	UserID     string    `json:"user_id"`
	Amount     float64   `json:"amount"`
	Currency   string    `json:"currency"`
	Merchant   string    `json:"merchant"`
	Location   string    `json:"location"`
	Timestamp  time.Time `json:"timestamp"`
	CardLast4  string    `json:"card_last4"`
	DeviceHash string    `json:"device_hash"`
}

type FraudAlert struct {
	ID          string      `json:"id"`
	Transaction Transaction `json:"transaction"`
	RiskScore   float64     `json:"risk_score"`
	RiskLevel   RiskLevel   `json:"risk_level"`
	Signals     []string    `json:"signals"`
	Status      string      `json:"status"` // pending | calling | confirmed_fraud | confirmed_legit | escalated | timeout
	CreatedAt   time.Time   `json:"created_at"`
	ResolvedAt  *time.Time  `json:"resolved_at,omitempty"`
}

type CallOutcome struct {
	AlertID    string `json:"alert_id"`
	Outcome    string `json:"outcome"` // confirmed_fraud | confirmed_legit | no_answer | escalated
	VerifiedBy string `json:"verified_by"` // mfa | voice | pin
}

// --- Risk Engine ---

type RiskEngine struct {
	rules []RiskRule
}

type RiskRule struct {
	Name    string
	Evaluate func(tx Transaction) (float64, string)
}

func NewRiskEngine() *RiskEngine {
	return &RiskEngine{
		rules: []RiskRule{
			{
				Name: "high_amount",
				Evaluate: func(tx Transaction) (float64, string) {
					if tx.Amount > 5000 {
						return 0.4, "transaction amount exceeds $5000"
					}
					if tx.Amount > 1000 {
						return 0.2, "transaction amount exceeds $1000"
					}
					return 0, ""
				},
			},
			{
				Name: "unusual_location",
				Evaluate: func(tx Transaction) (float64, string) {
					suspicious := map[string]bool{"NG": true, "RU": true, "CN": true, "BR": true}
					if suspicious[tx.Location] {
						return 0.3, fmt.Sprintf("transaction from high-risk region: %s", tx.Location)
					}
					return 0, ""
				},
			},
			{
				Name: "velocity",
				Evaluate: func(tx Transaction) (float64, string) {
					// In production: check tx count in last hour from Redis
					return 0, ""
				},
			},
			{
				Name: "odd_hours",
				Evaluate: func(tx Transaction) (float64, string) {
					hour := tx.Timestamp.Hour()
					if hour >= 1 && hour <= 5 {
						return 0.15, "transaction during unusual hours (1am-5am)"
					}
					return 0, ""
				},
			},
		},
	}
}

func (e *RiskEngine) Assess(tx Transaction) (float64, RiskLevel, []string) {
	var totalScore float64
	var signals []string

	for _, rule := range e.rules {
		score, signal := rule.Evaluate(tx)
		totalScore += score
		if signal != "" {
			signals = append(signals, signal)
		}
	}

	if totalScore > 1.0 {
		totalScore = 1.0
	}

	var level RiskLevel
	switch {
	case totalScore >= 0.7:
		level = RiskCritical
	case totalScore >= 0.5:
		level = RiskHigh
	case totalScore >= 0.3:
		level = RiskMedium
	default:
		level = RiskLow
	}

	return totalScore, level, signals
}

// --- Alert Store (in-memory, Redis in production) ---

type AlertStore struct {
	mu     sync.RWMutex
	alerts map[string]*FraudAlert
}

func NewAlertStore() *AlertStore {
	return &AlertStore{alerts: make(map[string]*FraudAlert)}
}

func (s *AlertStore) Save(alert *FraudAlert) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.alerts[alert.ID] = alert
}

func (s *AlertStore) Get(id string) (*FraudAlert, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.alerts[id]
	return a, ok
}

func (s *AlertStore) GetPending() []*FraudAlert {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var pending []*FraudAlert
	for _, a := range s.alerts {
		if a.Status == "pending" || a.Status == "calling" {
			pending = append(pending, a)
		}
	}
	return pending
}

func (s *AlertStore) All() []*FraudAlert {
	s.mu.RLock()
	defer s.mu.RUnlock()
	all := make([]*FraudAlert, 0, len(s.alerts))
	for _, a := range s.alerts {
		all = append(all, a)
	}
	return all
}

// --- Event Bus (simulates Kafka consumer/producer) ---

type EventBus struct {
	subscribers map[string][]chan []byte
	mu          sync.RWMutex
}

func NewEventBus() *EventBus {
	return &EventBus{subscribers: make(map[string][]chan []byte)}
}

func (b *EventBus) Subscribe(topic string) chan []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan []byte, 100)
	b.subscribers[topic] = append(b.subscribers[topic], ch)
	return ch
}

func (b *EventBus) Publish(topic string, data []byte) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, ch := range b.subscribers[topic] {
		select {
		case ch <- data:
		default:
			log.Printf("WARN: dropping message on topic %s (subscriber backpressure)", topic)
		}
	}
}

// --- Fraud Pipeline ---

type FraudPipeline struct {
	engine *RiskEngine
	store  *AlertStore
	bus    *EventBus
}

func NewFraudPipeline(engine *RiskEngine, store *AlertStore, bus *EventBus) *FraudPipeline {
	return &FraudPipeline{engine: engine, store: store, bus: bus}
}

func (p *FraudPipeline) Start(ctx context.Context) {
	txStream := p.bus.Subscribe("transactions")
	log.Println("Fraud pipeline listening on 'transactions' topic")

	for {
		select {
		case <-ctx.Done():
			log.Println("Fraud pipeline shutting down")
			return
		case raw := <-txStream:
			var tx Transaction
			if err := json.Unmarshal(raw, &tx); err != nil {
				log.Printf("ERROR: invalid transaction event: %v", err)
				continue
			}
			p.processTransaction(tx)
		}
	}
}

func (p *FraudPipeline) processTransaction(tx Transaction) {
	score, level, signals := p.engine.Assess(tx)

	if level == RiskLow {
		log.Printf("TX %s: score=%.2f level=%s — approved", tx.ID, score, level)
		return
	}

	alert := &FraudAlert{
		ID:          fmt.Sprintf("ALT-%s", tx.ID),
		Transaction: tx,
		RiskScore:   score,
		RiskLevel:   level,
		Signals:     signals,
		Status:      "pending",
		CreatedAt:   time.Now(),
	}
	p.store.Save(alert)

	log.Printf("ALERT %s: score=%.2f level=%s signals=%v", alert.ID, score, level, signals)

	if level == RiskCritical {
		alert.Status = "calling"
		p.store.Save(alert)
		// In production: trigger Twilio outbound call here
		log.Printf("CALLING user %s about alert %s", tx.UserID, alert.ID)
	}

	data, _ := json.Marshal(alert)
	p.bus.Publish("fraud_alerts", data)
}

// --- HTTP API ---

type Server struct {
	store  *AlertStore
	bus    *EventBus
	engine *RiskEngine
	pipe   *FraudPipeline
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /alerts", s.handleListAlerts)
	mux.HandleFunc("GET /alerts/{id}", s.handleGetAlert)
	mux.HandleFunc("POST /transactions", s.handleIngestTransaction)
	mux.HandleFunc("POST /alerts/{id}/resolve", s.handleResolveAlert)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleListAlerts(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.store.All())
}

func (s *Server) handleGetAlert(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	alert, ok := s.store.Get(id)
	if !ok {
		http.Error(w, `{"error":"alert not found"}`, 404)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(alert)
}

func (s *Server) handleIngestTransaction(w http.ResponseWriter, r *http.Request) {
	var tx Transaction
	if err := json.NewDecoder(r.Body).Decode(&tx); err != nil {
		http.Error(w, `{"error":"invalid request body"}`, 400)
		return
	}
	if tx.ID == "" {
		tx.ID = fmt.Sprintf("TX-%d", time.Now().UnixNano())
	}
	if tx.Timestamp.IsZero() {
		tx.Timestamp = time.Now()
	}

	data, _ := json.Marshal(tx)
	s.bus.Publish("transactions", data)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"status": "accepted", "transaction_id": tx.ID})
}

func (s *Server) handleResolveAlert(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	alert, ok := s.store.Get(id)
	if !ok {
		http.Error(w, `{"error":"alert not found"}`, 404)
		return
	}

	var outcome CallOutcome
	if err := json.NewDecoder(r.Body).Decode(&outcome); err != nil {
		http.Error(w, `{"error":"invalid request body"}`, 400)
		return
	}

	now := time.Now()
	alert.Status = outcome.Outcome
	alert.ResolvedAt = &now
	s.store.Save(alert)

	if outcome.Outcome == "confirmed_fraud" {
		log.Printf("FREEZE card %s for user %s", alert.Transaction.CardLast4, alert.Transaction.UserID)
		// In production: call card issuer API to freeze card
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(alert)
}

// --- Main ---

func main() {
	engine := NewRiskEngine()
	store := NewAlertStore()
	bus := NewEventBus()
	pipe := NewFraudPipeline(engine, store, bus)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go pipe.Start(ctx)

	srv := &Server{store: store, bus: bus, engine: engine, pipe: pipe}
	httpServer := &http.Server{
		Addr:    port(),
		Handler: srv.Routes(),
	}

	go func() {
		log.Printf("Fraud Mitigator API listening on %s", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	cancel()
	httpServer.Shutdown(context.Background())
}

func port() string {
	if p := os.Getenv("PORT"); p != "" {
		return ":" + p
	}
	return ":8080"
}
