// vgpu-scheduler — multiplexes {compute,mem} jobs across simulated GPU slots.
// Naive mode: 1 job → 1 whole GPU. Pack mode: first-fit bin-pack into residual capacity.
// Allocation state lives in Redis so the control plane is restart-safe.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type request struct {
	Compute float64 `json:"compute"` // 0–100 fraction of one GPU's compute
	Mem     float64 `json:"mem"`     // 0–100 fraction of one GPU's memory
	JobID   string  `json:"job_id,omitempty"`
}

type allocation struct {
	JobID   string  `json:"job_id"`
	Slot    int     `json:"slot"`
	Compute float64 `json:"compute"`
	Mem     float64 `json:"mem"`
}

type slotMetrics struct {
	Slot          int     `json:"slot"`
	ComputeUsed   float64 `json:"compute_used"`
	MemUsed       float64 `json:"mem_used"`
	ComputePct    float64 `json:"compute_pct"`
	MemPct        float64 `json:"mem_pct"`
	Jobs          int     `json:"jobs"`
	ExclusiveLock bool    `json:"exclusive_lock"` // set in naive mode when a job owns the slot
}

type metricsResponse struct {
	Mode               string        `json:"mode"`
	Slots              int           `json:"slots"`
	PoolComputePct     float64       `json:"pool_compute_pct"`
	PoolMemPct         float64       `json:"pool_mem_pct"`
	PoolUtilizationPct float64       `json:"pool_utilization_pct"`
	PerSlot            []slotMetrics `json:"per_slot"`
	Accepted           int           `json:"accepted"`
	Rejected           int           `json:"rejected"`
}

type scheduler struct {
	mu       sync.Mutex
	redis    string
	slots    int
	naive    bool
	accepted int
	rejected int
}

// chooseSlot contains the scheduling policy, kept separate from Redis and HTTP so
// the one interesting systems decision in this demo is easy to test and explain.
func chooseSlot(stats []slotMetrics, req request, naive bool) (int, error) {
	for _, st := range stats {
		if naive {
			if st.Jobs == 0 {
				return st.Slot, nil
			}
			continue
		}
		if !st.ExclusiveLock && st.ComputeUsed+req.Compute <= 100 && st.MemUsed+req.Mem <= 100 {
			return st.Slot, nil
		}
	}
	if naive {
		return -1, fmt.Errorf("no free GPU slot (naive 1:1)")
	}
	return -1, fmt.Errorf("no slot with residual capacity")
}

func main() {
	s := &scheduler{
		redis: env("REDIS_ADDR", "127.0.0.1:6379"),
		slots: envInt("GPU_SLOTS", 4),
		naive: env("MODE", "pack") == "naive",
	}
	if err := s.ensureSlots(); err != nil {
		log.Fatalf("redis init: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/allocate", s.handleAllocate)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/mode", s.handleMode)
	mux.HandleFunc("/reset", s.handleReset)

	addr := env("LISTEN", ":8080")
	log.Printf("vgpu-scheduler on %s  slots=%d  mode=%s  redis=%s",
		addr, s.slots, s.modeName(), s.redis)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func (s *scheduler) modeName() string {
	if s.naive {
		return "naive"
	}
	return "pack"
}

func (s *scheduler) handleMode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Naive *bool  `json:"naive"`
		Mode  string `json:"mode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case body.Naive != nil:
		s.naive = *body.Naive
	case body.Mode == "naive":
		s.naive = true
	case body.Mode == "pack":
		s.naive = false
	default:
		http.Error(w, `want {"mode":"naive"|"pack"} or {"naive":bool}`, http.StatusBadRequest)
		return
	}
	writeJSON(w, map[string]string{"mode": s.modeName()})
}

func (s *scheduler) handleReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.clearAll(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.accepted, s.rejected = 0, 0
	writeJSON(w, map[string]string{"status": "reset", "mode": s.modeName()})
}

func (s *scheduler) handleAllocate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if req.Compute <= 0 || req.Mem <= 0 || req.Compute > 100 || req.Mem > 100 {
		http.Error(w, "compute and mem must be in (0,100]", http.StatusBadRequest)
		return
	}
	if req.JobID == "" {
		req.JobID = fmt.Sprintf("job-%d", time.Now().UnixNano())
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	slot, err := s.place(req)
	if err != nil {
		s.rejected++
		w.WriteHeader(http.StatusConflict)
		writeJSON(w, map[string]any{"error": err.Error(), "job_id": req.JobID})
		return
	}
	s.accepted++
	writeJSON(w, allocation{JobID: req.JobID, Slot: slot, Compute: req.Compute, Mem: req.Mem})
}

func (s *scheduler) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	per, err := s.slotStats()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var sumC, sumM float64
	for _, m := range per {
		sumC += m.ComputeUsed
		sumM += m.MemUsed
	}
	cap := float64(s.slots) * 100
	pc, pm := 100*sumC/cap, 100*sumM/cap
	resp := metricsResponse{
		Mode:               s.modeName(),
		Slots:              s.slots,
		PoolComputePct:     round1(pc),
		PoolMemPct:         round1(pm),
		PoolUtilizationPct: round1((pc + pm) / 2),
		PerSlot:            per,
		Accepted:           s.accepted,
		Rejected:           s.rejected,
	}
	writeJSON(w, resp)
}

// place reads current capacity, chooses a slot, then persists the allocation.
// The handler's mutex makes this read/choose/write sequence atomic in this
// deliberately single-replica control-plane demo.
func (s *scheduler) place(req request) (int, error) {
	stats, err := s.slotStats()
	if err != nil {
		return -1, err
	}
	slot, err := chooseSlot(stats, req, s.naive)
	if err != nil {
		return -1, err
	}
	if err := s.commit(slot, req, s.naive); err != nil {
		return -1, err
	}
	return slot, nil
}

func (s *scheduler) commit(slot int, req request, exclusive bool) error {
	key := s.slotKey(slot)
	fields := map[string]string{
		"job:" + req.JobID: fmt.Sprintf("%.2f,%.2f", req.Compute, req.Mem),
	}
	if exclusive {
		fields["exclusive"] = "1"
	}
	return redisHSet(s.redis, key, fields)
}

func (s *scheduler) slotStats() ([]slotMetrics, error) {
	out := make([]slotMetrics, s.slots)
	for i := 0; i < s.slots; i++ {
		m := slotMetrics{Slot: i}
		vals, err := redisHGetAll(s.redis, s.slotKey(i))
		if err != nil {
			return nil, err
		}
		for k, v := range vals {
			if k == "exclusive" {
				m.ExclusiveLock = v == "1"
				continue
			}
			if !strings.HasPrefix(k, "job:") {
				continue
			}
			var c, mem float64
			if _, err := fmt.Sscanf(v, "%f,%f", &c, &mem); err != nil {
				continue
			}
			m.ComputeUsed += c
			m.MemUsed += mem
			m.Jobs++
		}
		m.ComputePct = round1(m.ComputeUsed)
		m.MemPct = round1(m.MemUsed)
		out[i] = m
	}
	return out, nil
}

func (s *scheduler) ensureSlots() error {
	if err := redisPing(s.redis); err != nil {
		return err
	}
	for i := 0; i < s.slots; i++ {
		// Touch each key so empty slots exist as hashes (HSET nx-style via empty ensure).
		_ = redisHSet(s.redis, s.slotKey(i), map[string]string{"_init": "1"})
		_ = redisHDel(s.redis, s.slotKey(i), "_init")
	}
	return nil
}

func (s *scheduler) clearAll() error {
	for i := 0; i < s.slots; i++ {
		if err := redisDel(s.redis, s.slotKey(i)); err != nil {
			return err
		}
	}
	return s.ensureSlots()
}

func (s *scheduler) slotKey(i int) string {
	return fmt.Sprintf("vgpu:slot:%d", i)
}

// --- minimal Redis over RESP (no third-party client) ---

func redisPing(addr string) error {
	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		return err
	}
	buf := make([]byte, 16)
	n, _ := conn.Read(buf)
	if !strings.Contains(string(buf[:n]), "PONG") {
		return fmt.Errorf("unexpected PING reply: %q", buf[:n])
	}
	return nil
}

func redisHSet(addr, key string, fields map[string]string) error {
	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	args := []string{"HSET", key}
	for k, v := range fields {
		args = append(args, k, v)
	}
	if _, err := conn.Write([]byte(respArray(args))); err != nil {
		return err
	}
	return readOK(conn)
}

func redisHDel(addr, key, field string) error {
	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.Write([]byte(respArray([]string{"HDEL", key, field}))); err != nil {
		return err
	}
	return readOK(conn)
}

func redisDel(addr, key string) error {
	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.Write([]byte(respArray([]string{"DEL", key}))); err != nil {
		return err
	}
	return readOK(conn)
}

func redisHGetAll(addr, key string) (map[string]string, error) {
	conn, err := dial(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if _, err := conn.Write([]byte(respArray([]string{"HGETALL", key}))); err != nil {
		return nil, err
	}
	return readStringMap(conn)
}

func dial(addr string) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	return conn, nil
}

func respArray(args []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	return b.String()
}

func readOK(conn net.Conn) error {
	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil {
		return err
	}
	line := string(buf[:n])
	if strings.HasPrefix(line, "-") {
		return fmt.Errorf("redis: %s", strings.TrimSpace(line))
	}
	return nil
}

// readStringMap parses a Redis array reply of alternating bulk strings into a map.
func readStringMap(conn net.Conn) (map[string]string, error) {
	raw, err := readAll(conn)
	if err != nil {
		return nil, err
	}
	parts := strings.Split(raw, "\r\n")
	out := map[string]string{}
	// RESP: *N, then N times ($len, value). Skip empties from trailing split.
	i := 0
	for i < len(parts) && parts[i] == "" {
		i++
	}
	if i >= len(parts) || !strings.HasPrefix(parts[i], "*") {
		return out, nil
	}
	n, _ := strconv.Atoi(parts[i][1:])
	i++
	vals := make([]string, 0, n)
	for len(vals) < n && i < len(parts) {
		if !strings.HasPrefix(parts[i], "$") {
			i++
			continue
		}
		i++ // move to value line
		if i >= len(parts) {
			break
		}
		vals = append(vals, parts[i])
		i++
	}
	for j := 0; j+1 < len(vals); j += 2 {
		out[vals[j]] = vals[j+1]
	}
	return out, nil
}

func readAll(conn net.Conn) (string, error) {
	var b strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			b.Write(buf[:n])
		}
		if err != nil {
			break
		}
		// Heuristic: HGETALL for our small hashes fits one read; if not, deadline ends it.
		if n < len(buf) {
			break
		}
	}
	return b.String(), nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func round1(f float64) float64 {
	return float64(int(f*10+0.5)) / 10
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
