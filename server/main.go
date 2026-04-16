// newradar — Single-File Web Application для отображения метеорадарных данных Росгидромета
package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"hash/fnv"
	"image"
	"image/color"
	"image/draw"
	"image/gif"
	_ "image/jpeg"
	"image/png"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Rad GIF source, refresh every 10 min. Кадры каждые 10 мин, гиф с задержкой 20 мин (последний кадр = now-20 мин по сетке :00/:10/.../:50).
const (
	radarURL           = "https://meteoinfo.ru/hmc-output/rmap/phenomena.gif"
	refreshPeriod      = 10 * time.Minute
	frameIntervalMin   = 10
	gifDelayMin        = 20
	referenceFile      = "../reference_map.png"
	referenceGeo       = "../reference_map_modified.tif"
	warpedMaskFileName = "mask_runtime_warped.png"
	pointsFile         = "../reference_map.png.points"
	manualRadarsFile   = "../radars_manual.json"
	maskFile           = "../mask.png"
	cityMaskFile       = "../city_mask.png"
	framesDir          = "../frames"
	warpedDir          = "../frames_warped"
	tmpWarpDir         = "../tmp_warp"
	clientStaticDir    = "../client/static"
	framePrefix        = "frame_"
	lightningFreeBase  = "https://www.limaps.org/JSON/C1"
	lightningGLD360URL = "https://boxtools.space/all/GLD360.php"
	nowcastWMSBaseURL  = "https://www.nowcast.ru/baltrad_wsgi"
	nowcastTokenURL    = "https://www.nowcast.ru/get_token"
	nowcastReferer     = "https://www.nowcast.ru/RAD/demo.html"
	nowcastUserAgent   = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
	nowcastCacheDir    = "../cache/nowcast"
	eumetviewWMSBase   = "https://view.eumetsat.int/geoserver/ows"
	eumetTokenURL      = "https://gateway.apis.eumetsat.int/token"
	eumetTokenURLAlt   = "https://api.eumetsat.int/token"
	eumetDefaultLayer  = "msg_fes:rgb_eview"
	satelliteCacheDir  = "../cache/satellite"
	syncFrameLimit     = 18
	syncRequestTimeout = 95 * time.Second
	meteoFrameWindow   = 19
)

var nowcastSyncLayers = []string{
	"bufr_phenomena",
	"bufr_height",
	"bufr_dbz1",
	"bufr_precip",
}

// Georeference control points (lat, lon) -> (pixel x, y)
// Moscow: [55.7558, 37.6173] -> [502, 381]
// Arkhangelsk: [64.5472, 40.5602] -> [812, 145]
// Image dimensions are detected at runtime
const (
	moscowLat, moscowLon       = 55.7558, 37.6173
	moscowPx, moscowPy         = 502, 381
	arkhangelskLat, arkhangLon = 64.5472, 40.5602
	arkhangPx, arkhangPy       = 812, 145
)

type frameInfo struct {
	URL   string `json:"url"`
	Time  string `json:"time"`
	Index int    `json:"index"`
}

type radarState struct {
	mu               sync.RWMutex
	frames           []frameInfo
	imageSize        [2]int // width, height
	geoBounds        [4]float64
	radars           []radarSite
	initialLoadDone  bool
	lastFrameDigest  uint64
	hasLastFrameHash bool
	lastUpdate       time.Time
}

var state radarState

type radarSite struct {
	Lon      float64 `json:"lon"`
	Lat      float64 `json:"lat"`
	RadiusKm float64 `json:"radiusKm"`
}

type lightningStrike struct {
	Time string  `json:"time"`
	Lat  float64 `json:"lat"`
	Lon  float64 `json:"lon"`
	Pol  int     `json:"pol"`
}

type gld360FeatureCollection struct {
	Station  string `json:"station"`
	Features []struct {
		Type     string `json:"type"`
		Geometry struct {
			Type        string    `json:"type"`
			Coordinates []float64 `json:"coordinates"`
		} `json:"geometry"`
		Properties struct {
			Date string `json:"date"`
		} `json:"properties"`
	} `json:"features"`
}

var (
	thinSplineEnabled  = false
	thinSplineState    thinSplineRuntime
	lightningHTTP      = &http.Client{Timeout: 20 * time.Second}
	nowcastHTTP        = &http.Client{Timeout: 35 * time.Second}
	satelliteHTTP      = &http.Client{Timeout: 35 * time.Second}
	gld360CacheMu      sync.Mutex
	gld360CacheKey     string
	gld360CacheUntil   time.Time
	gld360CacheStrikes []lightningStrike
	gld360WarnMu       sync.Mutex
	gld360WarnUntil    = map[string]time.Time{}
	nowcastTokenMu     sync.Mutex
	nowcastAccessToken string
	nowcastTokenExpiry time.Time
	eumetTokenMu       sync.Mutex
	eumetAccessToken   string
	eumetTokenExpiry   time.Time
)

type wmsCapabilities struct {
	Capability struct {
		Layer wmsLayer `xml:"Layer"`
	} `xml:"Capability"`
}

type wmsLayer struct {
	Name       string      `xml:"Name"`
	Title      string      `xml:"Title"`
	Dimensions []wmsDimDef `xml:"Dimension"`
	Extents    []wmsDimDef `xml:"Extent"`
	Layers     []wmsLayer  `xml:"Layer"`
}

type wmsDimDef struct {
	Name string `xml:"name,attr"`
	Text string `xml:",chardata"`
}

type nowcastLayerInfo struct {
	Name      string `json:"name"`
	Title     string `json:"title"`
	TimeCount int    `json:"timeCount"`
}

type nowcastFrameInfo struct {
	URL         string     `json:"url"`
	Time        string     `json:"time"`
	Projection  string     `json:"projection"`
	ImageExtent [4]float64 `json:"imageExtent"`
}

type satelliteAvailableEntry struct {
	Params struct {
		Area string `json:"area"`
		Size string `json:"size"`
		Time string `json:"time"`
		Type string `json:"type"`
	} `json:"params"`
	URI string `json:"uri"`
}

type satelliteFrameInfo struct {
	URL         string     `json:"url"`
	Time        string     `json:"time"`
	Projection  string     `json:"projection"`
	ImageExtent [4]float64 `json:"imageExtent"`
}

type eumetTokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
}

type nowcastTokenResponse struct {
	Token string `json:"token"`
}

type thinSplineRuntime struct {
	GCPArgs          []string
	GridW            int
	GridH            int
	MinX             float64
	MinY             float64
	MaxX             float64
	MaxY             float64
	GDALInfoCmd      string
	GDALTranslateCmd string
	GDALWarpCmd      string
}

type radarCandidate struct {
	x int
	y int
	r float64
}

type gcpQualityReport struct {
	EnabledCount       int
	SourceMinX         float64
	SourceMinY         float64
	SourceMaxX         float64
	SourceMaxY         float64
	SourceSpanX        float64
	SourceSpanY        float64
	ResidualMean       float64
	ResidualMax        float64
	ResidualSampleSize int
}

type frameBatchMetrics struct {
	CoverageMean   float64
	FlickerRate    float64
	EdgeRoughness  float64
	MeanAlpha      float64
	FrameCount     int
	PixelSamples   int
	TemporalPairs  int
	BoundaryChecks int
}

func main() {
	loadDotEnvIntoProcess([]string{
		".env",
		"../.env",
	})

	if err := os.MkdirAll(framesDir, 0755); err != nil {
		log.Fatalf("Cannot create frames dir: %v", err)
	}
	if err := os.MkdirAll(warpedDir, 0755); err != nil {
		log.Fatalf("Cannot create warped frames dir: %v", err)
	}
	if err := os.MkdirAll(tmpWarpDir, 0755); err != nil {
		log.Fatalf("Cannot create tmp warp dir: %v", err)
	}
	if err := os.MkdirAll(nowcastCacheDir, 0755); err != nil {
		log.Fatalf("Cannot create nowcast cache dir: %v", err)
	}
	if err := os.MkdirAll(satelliteCacheDir, 0755); err != nil {
		log.Fatalf("Cannot create satellite cache dir: %v", err)
	}
	if err := initThinSplineRuntime(); err != nil {
		log.Printf("Thin-spline runtime disabled: %v", err)
	} else {
		thinSplineEnabled = true
		log.Printf("Thin-spline runtime enabled: %d GCP, grid=%dx%d", len(thinSplineState.GCPArgs)/5, thinSplineState.GridW, thinSplineState.GridH)
		autoRadars, err := detectRadarsFromMask()
		if err != nil {
			log.Printf("Radar detection warning: %v", err)
		} else {
			state.mu.Lock()
			state.radars = autoRadars
			state.mu.Unlock()
			log.Printf("Detected radar sites (auto): %d", len(autoRadars))
		}

		if manual, err := loadManualRadars(autoRadars); err != nil {
			if !os.IsNotExist(err) {
				log.Printf("Manual radar file warning: %v", err)
			}
		} else if len(manual) > 0 {
			state.mu.Lock()
			state.radars = manual
			state.mu.Unlock()
			log.Printf("Using manual radar centers: %d (file: %s)", len(manual), manualRadarsFile)
		}
	}
	initStateFromExistingFrames()
	if parseBoolEnv("BLOCKING_WARMUP", true) {
		if err := runBlockingWarmup(); err != nil {
			log.Fatalf("Blocking warmup failed: %v", err)
		}
	} else {
		// Initial fetch
		go fetchAndProcess()
		go syncExternalSources("startup")
	}

	// Periodic fetch
	ticker := time.NewTicker(refreshPeriod)
	go func() {
		for range ticker.C {
			fetchAndProcess()
			syncExternalSources("periodic")
		}
	}()

	// Routes
	http.HandleFunc("/", handleIndex)
	http.HandleFunc("/api/radar/latest", handleLatest)
	http.HandleFunc("/api/lightning/free/latest", handleLightningFreeLatest)
	http.HandleFunc("/api/lightning/gld360/latest", handleLightningGLD360Latest)
	http.HandleFunc("/api/nowcast/meta", handleNowcastMeta)
	http.HandleFunc("/api/nowcast/frames", handleNowcastFrames)
	http.HandleFunc("/api/nowcast/wms", handleNowcastWMSProxy)
	http.HandleFunc("/api/satellite/frames", handleSatelliteFrames)
	http.HandleFunc("/api/satellite/image", handleSatelliteImageProxy)
	http.HandleFunc("/reference_map.png", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, referenceFile)
	})
	// Геопривязанная карта из QGIS (reference_map_modified.tif)
	http.Handle("/frames/", http.StripPrefix("/frames/", http.FileServer(http.Dir(framesDir))))
	http.Handle("/frames_warped/", http.StripPrefix("/frames_warped/", http.FileServer(http.Dir(warpedDir))))

	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir(clientStaticDir))))

	addr := ":8080"
	log.Printf("Radar server starting at http://localhost%s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal(err)
	}
}

func initStateFromExistingFrames() {
	existing := scanFrames()
	if len(existing) == 0 {
		log.Printf("[startup] local frames: none")
		return
	}

	w, h := detectFrameImageSize(existing[0].URL)
	digest, hasDigest := computeFrameDigestByURL(existing[len(existing)-1].URL)

	state.mu.Lock()
	state.frames = existing
	state.initialLoadDone = true
	state.lastUpdate = time.Now()
	if w > 0 && h > 0 {
		state.imageSize = [2]int{w, h}
	}
	if hasDigest {
		state.lastFrameDigest = digest
		state.hasLastFrameHash = true
	}
	state.mu.Unlock()

	log.Printf(
		"[startup] local frames restored: count=%d image=%dx%d digest=%t",
		len(existing),
		w,
		h,
		hasDigest,
	)
}

func detectFrameImageSize(frameURL string) (int, int) {
	path := frameURLToPath(frameURL)
	if strings.TrimSpace(path) == "" {
		return 0, 0
	}
	f, err := os.Open(path)
	if err != nil {
		return 0, 0
	}
	defer f.Close()
	cfg, err := png.DecodeConfig(f)
	if err != nil {
		return 0, 0
	}
	return cfg.Width, cfg.Height
}

func computeFrameDigestByURL(frameURL string) (uint64, bool) {
	path := frameURLToPath(frameURL)
	if strings.TrimSpace(path) == "" {
		return 0, false
	}
	f, err := os.Open(path)
	if err != nil {
		return 0, false
	}
	defer f.Close()
	img, err := png.Decode(f)
	if err != nil {
		return 0, false
	}
	b := img.Bounds()
	rgba := image.NewRGBA(b)
	draw.Draw(rgba, b, img, b.Min, draw.Src)
	return hashRGBA(rgba), true
}

func frameURLToPath(frameURL string) string {
	if strings.HasPrefix(frameURL, "/frames_warped/") {
		return filepath.Join(warpedDir, strings.TrimPrefix(frameURL, "/frames_warped/"))
	}
	if strings.HasPrefix(frameURL, "/frames/") {
		return filepath.Join(framesDir, strings.TrimPrefix(frameURL, "/frames/"))
	}
	return ""
}

func loadDotEnvIntoProcess(candidates []string) {
	for _, p := range candidates {
		path := strings.TrimSpace(p)
		if path == "" {
			continue
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		lines := strings.Split(string(raw), "\n")
		loaded := 0
		for _, line := range lines {
			s := strings.TrimSpace(line)
			if s == "" || strings.HasPrefix(s, "#") {
				continue
			}
			if strings.HasPrefix(s, "export ") {
				s = strings.TrimSpace(strings.TrimPrefix(s, "export "))
			}
			idx := strings.IndexByte(s, '=')
			if idx <= 0 {
				continue
			}
			key := strings.TrimSpace(s[:idx])
			val := strings.TrimSpace(s[idx+1:])
			if key == "" {
				continue
			}
			// Не перетираем уже выставленные env переменные.
			if _, exists := os.LookupEnv(key); exists {
				continue
			}
			// Снимаем парные кавычки.
			if len(val) >= 2 {
				if (val[0] == '"' && val[len(val)-1] == '"') || (val[0] == '\'' && val[len(val)-1] == '\'') {
					val = val[1 : len(val)-1]
				}
			}
			// Для unquoted значений убираем inline-комментарий.
			if !strings.HasPrefix(val, "\"") && !strings.HasPrefix(val, "'") {
				if c := strings.Index(val, " #"); c >= 0 {
					val = strings.TrimSpace(val[:c])
				}
			}
			_ = os.Setenv(key, val)
			loaded++
		}
		log.Printf("Loaded .env from %s (%d keys)", path, loaded)
		return
	}
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		http.ServeFile(w, r, filepath.Join(clientStaticDir, "index.html"))
		return
	}
	// Serve other root paths (e.g. /index.html) from static
	name := strings.TrimPrefix(r.URL.Path, "/")
	if name == "" || strings.Contains(name, "..") {
		http.NotFound(w, r)
		return
	}
	http.ServeFile(w, r, filepath.Join(clientStaticDir, name))
}

func handleLatest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	state.mu.RLock()
	defer state.mu.RUnlock()

	out := struct {
		Frames     []frameInfo `json:"frames"`
		ImageSize  [2]int      `json:"imageSize"`
		GeoBounds  [4]float64  `json:"geoBounds"`
		Radars     []radarSite `json:"radars"`
		LastUpdate string      `json:"lastUpdate"`
	}{
		Frames:     state.frames,
		ImageSize:  state.imageSize,
		GeoBounds:  state.geoBounds,
		Radars:     state.radars,
		LastUpdate: state.lastUpdate.Format(time.RFC3339),
	}

	_ = json.NewEncoder(w).Encode(out)
}

func handleLightningFreeLatest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	now := time.Now().UTC()
	minutes := parseQueryInt(r, "minutes", 20, 5, 180)
	limit := parseQueryInt(r, "limit", 1500, 100, 10000)
	start := now.Add(-time.Duration(minutes) * time.Minute)

	west, hasWest := parseQueryFloat(r, "west")
	east, hasEast := parseQueryFloat(r, "east")
	south, hasSouth := parseQueryFloat(r, "south")
	north, hasNorth := parseQueryFloat(r, "north")
	useBBox := hasWest && hasEast && hasSouth && hasNorth && east > west && north > south

	fromBucket := floorToTenMinutes(start)
	toBucket := floorToTenMinutes(now)
	var out []lightningStrike
	authBlocked := false
	for ts := fromBucket; !ts.After(toBucket); ts = ts.Add(10 * time.Minute) {
		part, blocked, err := fetchFreeLightningBucket(r.Context(), ts, start, now, useBBox, west, east, south, north)
		if blocked {
			authBlocked = true
			break
		}
		if err != nil {
			log.Printf("Free lightning fetch warning: %v", err)
			continue
		}
		out = append(out, part...)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Time > out[j].Time })
	if len(out) > limit {
		out = out[:limit]
	}

	notice := ""
	if authBlocked && len(out) == 0 {
		notice = "Free limaps JSON is currently unauthorized (HTTP 401/403)."
	}
	resp := struct {
		Source  string            `json:"source"`
		From    string            `json:"from"`
		To      string            `json:"to"`
		Count   int               `json:"count"`
		Notice  string            `json:"notice,omitempty"`
		Strikes []lightningStrike `json:"strikes"`
	}{
		Source:  "Blitzortung free (limaps C1)",
		From:    start.Format(time.RFC3339),
		To:      now.Format(time.RFC3339),
		Count:   len(out),
		Notice:  notice,
		Strikes: out,
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func handleLightningGLD360Latest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	now := time.Now().UTC()
	minutes := parseQueryInt(r, "minutes", 20, 5, 180)
	limit := parseQueryInt(r, "limit", 3000, 100, 20000)
	start := now.Add(-time.Duration(minutes) * time.Minute)
	stations := parseGLD360Stations(r.URL.Query().Get("stations"))

	west, hasWest := parseQueryFloat(r, "west")
	east, hasEast := parseQueryFloat(r, "east")
	south, hasSouth := parseQueryFloat(r, "south")
	north, hasNorth := parseQueryFloat(r, "north")
	useBBox := hasWest && hasEast && hasSouth && hasNorth && east > west && north > south

	strikes, err := fetchGLD360LatestCached(
		r.Context(),
		stations,
		start,
		now,
		useBBox,
		west, east, south, north,
	)
	if err != nil {
		http.Error(w, fmt.Sprintf("gld360 error: %v", err), http.StatusBadGateway)
		return
	}
	sort.Slice(strikes, func(i, j int) bool { return strikes[i].Time > strikes[j].Time })
	if len(strikes) > limit {
		strikes = strikes[:limit]
	}
	resp := struct {
		Source   string            `json:"source"`
		Stations []string          `json:"stations"`
		From     string            `json:"from"`
		To       string            `json:"to"`
		Count    int               `json:"count"`
		Strikes  []lightningStrike `json:"strikes"`
	}{
		Source:   "GLD360 (boxtools.space)",
		Stations: stations,
		From:     start.Format(time.RFC3339),
		To:       now.Format(time.RFC3339),
		Count:    len(strikes),
		Strikes:  strikes,
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func parseGLD360Stations(raw string) []string {
	defaultStations := []string{"GLD2NE", "GLD3N", "GLD4S", "MTG1"}
	if strings.TrimSpace(raw) == "" {
		return defaultStations
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		station := strings.ToUpper(strings.TrimSpace(part))
		if station == "" {
			continue
		}
		if _, ok := seen[station]; ok {
			continue
		}
		seen[station] = struct{}{}
		out = append(out, station)
	}
	if len(out) == 0 {
		return defaultStations
	}
	return out
}

func fetchGLD360LatestCached(
	ctx context.Context,
	stations []string,
	start time.Time,
	end time.Time,
	useBBox bool,
	west float64,
	east float64,
	south float64,
	north float64,
) ([]lightningStrike, error) {
	window := end.Sub(start)
	if window <= 0 {
		window = 20 * time.Minute
	}
	// Квантование времени снимает "дребезг" ключа кэша и убирает шторм запросов.
	roundedEnd := end.UTC().Truncate(20 * time.Second)
	roundedStart := roundedEnd.Add(-window)
	cacheKey := fmt.Sprintf(
		"stations=%s|from=%s|to=%s|bbox=%t:%.4f,%.4f,%.4f,%.4f",
		strings.Join(stations, ","),
		roundedStart.Format(time.RFC3339),
		roundedEnd.Format(time.RFC3339),
		useBBox,
		west, east, south, north,
	)
	gld360CacheMu.Lock()
	if gld360CacheKey == cacheKey && time.Now().Before(gld360CacheUntil) {
		out := append([]lightningStrike(nil), gld360CacheStrikes...)
		gld360CacheMu.Unlock()
		return out, nil
	}
	gld360CacheMu.Unlock()

	out, err := fetchGLD360Latest(ctx, stations, start, end, useBBox, west, east, south, north)
	if err != nil {
		return nil, err
	}
	gld360CacheMu.Lock()
	gld360CacheKey = cacheKey
	gld360CacheUntil = time.Now().Add(20 * time.Second)
	gld360CacheStrikes = append([]lightningStrike(nil), out...)
	gld360CacheMu.Unlock()
	return out, nil
}

func fetchGLD360Latest(
	ctx context.Context,
	stations []string,
	start time.Time,
	end time.Time,
	useBBox bool,
	west float64,
	east float64,
	south float64,
	north float64,
) ([]lightningStrike, error) {
	all := make([]lightningStrike, 0, 4096)
	seen := make(map[string]struct{}, 8192)
	for _, station := range stations {
		part, err := fetchGLD360Station(ctx, station, start, end, useBBox, west, east, south, north)
		if err != nil {
			logGLD360WarningThrottled(station, err)
			continue
		}
		for _, s := range part {
			key := fmt.Sprintf("%s|%.4f|%.4f", s.Time, s.Lat, s.Lon)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			all = append(all, s)
		}
	}
	if len(all) == 0 {
		return []lightningStrike{}, nil
	}
	return all, nil
}

func logGLD360WarningThrottled(station string, err error) {
	msg := strings.TrimSpace(fmt.Sprintf("%v", err))
	if msg == "" {
		return
	}
	key := strings.ToUpper(strings.TrimSpace(station)) + "|" + msg
	now := time.Now()
	gld360WarnMu.Lock()
	until := gld360WarnUntil[key]
	if now.Before(until) {
		gld360WarnMu.Unlock()
		return
	}
	gld360WarnUntil[key] = now.Add(45 * time.Second)
	if len(gld360WarnUntil) > 128 {
		for k, exp := range gld360WarnUntil {
			if now.After(exp) {
				delete(gld360WarnUntil, k)
			}
		}
	}
	gld360WarnMu.Unlock()
	log.Printf("[lightning/gld360] station=%s warning: %s", station, msg)
}

func fetchGLD360Station(
	ctx context.Context,
	station string,
	start time.Time,
	end time.Time,
	useBBox bool,
	west float64,
	east float64,
	south float64,
	north float64,
) ([]lightningStrike, error) {
	payload, err := json.Marshal(map[string]string{"station": station})
	if err != nil {
		return nil, err
	}
	endpoints := []string{
		lightningGLD360URL,
		"https://www.boxtools.space/all/GLD360.php",
	}
	lastErr := error(nil)
	var fc gld360FeatureCollection
	for _, endpoint := range endpoints {
		if strings.TrimSpace(endpoint) == "" {
			continue
		}
		req, reqErr := http.NewRequestWithContext(
			ctx,
			http.MethodPost,
			endpoint,
			bytes.NewReader(payload),
		)
		if reqErr != nil {
			lastErr = reqErr
			continue
		}
		req.Header.Set("User-Agent", nowcastUserAgent)
		req.Header.Set("Accept", "*/*")
		req.Header.Set("Accept-Language", "ru-RU,ru;q=0.9")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", "https://boxtools.space")
		req.Header.Set("Referer", "https://boxtools.space/all/GLD360.php")
		req.Header.Set("Sec-Fetch-Dest", "empty")
		req.Header.Set("Sec-Fetch-Mode", "cors")
		req.Header.Set("Sec-Fetch-Site", "same-origin")
		resp, doErr := lightningHTTP.Do(req)
		if doErr != nil {
			lastErr = doErr
			if errors.Is(doErr, context.Canceled) || errors.Is(doErr, context.DeadlineExceeded) {
				return nil, doErr
			}
			var dnsErr *net.DNSError
			if errors.As(doErr, &dnsErr) {
				continue
			}
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			lastErr = readErr
			continue
		}
		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("station %s: endpoint=%s HTTP %d", station, endpoint, resp.StatusCode)
			continue
		}
		if err := json.Unmarshal(body, &fc); err != nil {
			lastErr = fmt.Errorf("station %s: endpoint=%s decode: %w", station, endpoint, err)
			continue
		}
		lastErr = nil
		break
	}
	if lastErr != nil {
		return nil, lastErr
	}
	out := make([]lightningStrike, 0, len(fc.Features))
	for _, f := range fc.Features {
		if len(f.Geometry.Coordinates) < 2 {
			continue
		}
		lon := f.Geometry.Coordinates[0]
		lat := f.Geometry.Coordinates[1]
		if !NumberIsFinite(lat) || !NumberIsFinite(lon) {
			continue
		}
		t, ok := parseGLD360Time(f.Properties.Date)
		if !ok || t.Before(start) || t.After(end.Add(2*time.Minute)) {
			continue
		}
		if useBBox && (lon < west || lon > east || lat < south || lat > north) {
			continue
		}
		out = append(out, lightningStrike{
			Time: t.Format(time.RFC3339Nano),
			Lat:  lat,
			Lon:  lon,
			Pol:  0,
		})
	}
	return out, nil
}

func parseGLD360Time(raw string) (time.Time, bool) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return time.Time{}, false
	}
	layouts := []string{
		"2006-01-02 15:04:05-07",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05 MST",
		"2006-01-02 15:04 MST",
		time.RFC3339,
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, v); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

func fetchFreeLightningBucket(
	ctx context.Context,
	ts time.Time,
	start time.Time,
	end time.Time,
	useBBox bool,
	west float64,
	east float64,
	south float64,
	north float64,
) ([]lightningStrike, bool, error) {
	plainURL := fmt.Sprintf("%s/%04d/%02d/%02d/%02d/%02d.json", lightningFreeBase, ts.Year(), ts.Month(), ts.Day(), ts.Hour(), ts.Minute())
	gzURL := plainURL + ".gz"
	urls := []string{plainURL, gzURL}

	authBlocked := false
	var lastErr error
	for _, fileURL := range urls {
		body, status, err := fetchLightningBody(ctx, fileURL)
		if status == http.StatusUnauthorized || status == http.StatusForbidden {
			authBlocked = true
			continue
		}
		if err != nil {
			lastErr = err
			continue
		}
		if strings.HasSuffix(fileURL, ".gz") {
			zr, zerr := gzip.NewReader(bytes.NewReader(body))
			if zerr != nil {
				lastErr = fmt.Errorf("%s: gzip reader: %w", fileURL, zerr)
				continue
			}
			decompressed, rerr := io.ReadAll(zr)
			_ = zr.Close()
			if rerr != nil {
				lastErr = fmt.Errorf("%s: gzip read: %w", fileURL, rerr)
				continue
			}
			body = decompressed
		}
		return parseLightningLines(body, start, end, useBBox, west, east, south, north), false, nil
	}
	if authBlocked {
		return nil, true, fmt.Errorf("upstream auth required")
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no data source available for %s", ts.Format(time.RFC3339))
	}
	return nil, false, lastErr
}

func fetchLightningBody(ctx context.Context, url string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("User-Agent", "newradar/1.0 (+https://localhost)")
	req.Header.Set("Accept", "application/json,text/plain,*/*")
	resp, err := lightningHTTP.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode, fmt.Errorf("%s: HTTP %d", url, resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return data, resp.StatusCode, nil
}

func parseLightningLines(
	data []byte,
	start time.Time,
	end time.Time,
	useBBox bool,
	west float64,
	east float64,
	south float64,
	north float64,
) []lightningStrike {
	type rawStrike struct {
		Time int64   `json:"time"`
		Lat  float64 `json:"lat"`
		Lon  float64 `json:"lon"`
		Pol  int     `json:"pol"`
	}
	out := make([]lightningStrike, 0, 256)
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 0, 1024), 4*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var raw rawStrike
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			continue
		}
		if !NumberIsFinite(raw.Lat) || !NumberIsFinite(raw.Lon) {
			continue
		}
		t := time.Unix(0, raw.Time).UTC()
		if t.Before(start) || t.After(end.Add(2*time.Minute)) {
			continue
		}
		if useBBox && (raw.Lon < west || raw.Lon > east || raw.Lat < south || raw.Lat > north) {
			continue
		}
		out = append(out, lightningStrike{
			Time: t.Format(time.RFC3339Nano),
			Lat:  raw.Lat,
			Lon:  raw.Lon,
			Pol:  raw.Pol,
		})
	}
	return out
}

func floorToTenMinutes(t time.Time) time.Time {
	t = t.UTC()
	minute := (t.Minute() / 10) * 10
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), minute, 0, 0, time.UTC)
}

func parseQueryInt(r *http.Request, key string, def int, min int, max int) int {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func parseQueryFloat(r *http.Request, key string) (float64, bool) {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || !NumberIsFinite(v) {
		return 0, false
	}
	return v, true
}

func parseQueryFloatOrDefault(r *http.Request, key string, def float64) float64 {
	v, ok := parseQueryFloat(r, key)
	if ok {
		return v
	}
	return def
}

func handleNowcastMeta(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	w.Header().Set("Content-Type", "application/json")
	log.Printf("[nowcast/meta] request: layers=%q", strings.TrimSpace(r.URL.Query().Get("layers")))
	capXML, err := getNowcastCapabilitiesCached(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("capabilities error: %v", err), http.StatusBadGateway)
		log.Printf("[nowcast/meta] error: capabilities: %v", err)
		return
	}

	layerTimes, layerTitles, allLayers, err := parseNowcastCapabilities(capXML)
	if err != nil {
		http.Error(w, fmt.Sprintf("capabilities parse error: %v", err), http.StatusBadGateway)
		log.Printf("[nowcast/meta] error: capabilities parse: %v", err)
		return
	}

	selected := strings.TrimSpace(r.URL.Query().Get("layers"))
	if selected == "" {
		for _, li := range allLayers {
			if li.TimeCount > 0 {
				selected = li.Name
				break
			}
		}
		if selected == "" && len(allLayers) > 0 {
			selected = allLayers[0].Name
		}
	}
	times := intersectLayerTimes(selected, layerTimes)
	latest := ""
	if len(times) > 0 {
		latest = times[len(times)-1]
	}

	out := struct {
		URL            string             `json:"url"`
		Version        string             `json:"version"`
		DefaultLayers  string             `json:"defaultLayers"`
		DefaultCRS     string             `json:"defaultCRS"`
		Layers         []nowcastLayerInfo `json:"layers"`
		SelectedTitle  string             `json:"selectedTitle,omitempty"`
		AvailableTimes []string           `json:"availableTimes"`
		LatestTime     string             `json:"latestTime,omitempty"`
	}{
		URL:            "/api/nowcast/wms",
		Version:        "1.3.0",
		DefaultLayers:  selected,
		DefaultCRS:     "EPSG:3857",
		Layers:         allLayers,
		SelectedTitle:  layerTitles[selected],
		AvailableTimes: times,
		LatestTime:     latest,
	}
	_ = json.NewEncoder(w).Encode(out)
	log.Printf(
		"[nowcast/meta] done: selected=%q layers=%d times=%d latest=%q elapsed=%s",
		selected,
		len(allLayers),
		len(times),
		latest,
		time.Since(started).Round(time.Millisecond),
	)
}

func handleNowcastFrames(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	w.Header().Set("Content-Type", "application/json")
	layer := strings.TrimSpace(r.URL.Query().Get("layer"))
	if layer == "" {
		http.Error(w, "layer is required", http.StatusBadRequest)
		log.Printf("[nowcast/frames] bad request: missing layer")
		return
	}

	version := strings.TrimSpace(r.URL.Query().Get("version"))
	if version == "" {
		version = "1.3.0"
	}
	crs := strings.TrimSpace(r.URL.Query().Get("crs"))
	if crs == "" {
		crs = "EPSG:3857"
	}
	log.Printf("[nowcast/frames] request: layer=%q version=%s crs=%s", layer, version, crs)

	state.mu.RLock()
	defaultBounds := state.geoBounds
	defaultSize := state.imageSize
	state.mu.RUnlock()

	west := parseQueryFloatOrDefault(r, "west", defaultBounds[0])
	south := parseQueryFloatOrDefault(r, "south", defaultBounds[1])
	east := parseQueryFloatOrDefault(r, "east", defaultBounds[2])
	north := parseQueryFloatOrDefault(r, "north", defaultBounds[3])
	if !NumberIsFinite(west) || !NumberIsFinite(south) || !NumberIsFinite(east) || !NumberIsFinite(north) || east <= west || north <= south {
		http.Error(w, "invalid bounds", http.StatusBadRequest)
		log.Printf("[nowcast/frames] bad request: invalid bounds")
		return
	}

	width := parseQueryInt(r, "width", max(1, defaultSize[0]), 64, 4096)
	height := parseQueryInt(r, "height", max(1, defaultSize[1]), 64, 4096)
	if width <= 0 {
		width = 1024
	}
	if height <= 0 {
		height = 768
	}

	capXML, err := getNowcastCapabilitiesCached(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("capabilities error: %v", err), http.StatusBadGateway)
		log.Printf("[nowcast/frames] error: capabilities: %v", err)
		return
	}
	layerTimes, _, _, err := parseNowcastCapabilities(capXML)
	if err != nil {
		http.Error(w, fmt.Sprintf("capabilities parse error: %v", err), http.StatusBadGateway)
		log.Printf("[nowcast/frames] error: capabilities parse: %v", err)
		return
	}
	times := intersectLayerTimes(layer, layerTimes)
	if len(times) == 0 {
		http.Error(w, "no available times for selected layer", http.StatusBadRequest)
		log.Printf("[nowcast/frames] no times for layer=%q", layer)
		return
	}
	limit := parseQueryInt(r, "limit", 18, 1, 72)
	log.Printf(
		"[nowcast/frames] resolved params: layer=%q times_available=%d limit=%d size=%dx%d bounds=[%.4f %.4f %.4f %.4f]",
		layer, len(times), limit, width, height, west, south, east, north,
	)
	if len(times) > limit {
		times = times[len(times)-limit:]
	}

	imageExtent := [4]float64{west, south, east, north}
	bbox := fmt.Sprintf("%.10f,%.10f,%.10f,%.10f", west, south, east, north)
	if strings.EqualFold(crs, "EPSG:3857") {
		minX, minY := lonLatToWebMercator(west, south)
		maxX, maxY := lonLatToWebMercator(east, north)
		imageExtent = [4]float64{minX, minY, maxX, maxY}
		bbox = fmt.Sprintf("%.3f,%.3f,%.3f,%.3f", minX, minY, maxX, maxY)
	}

	frames := make([]nowcastFrameInfo, 0, len(times))
	cacheHit := 0
	cacheMiss := 0
	warmQueued := 0
	misses := make([]string, 0, len(times))
	for _, t := range times {
		values := url.Values{}
		values.Set("SERVICE", "WMS")
		values.Set("VERSION", version)
		values.Set("REQUEST", "GetMap")
		values.Set("FORMAT", "image/png")
		values.Set("TRANSPARENT", "TRUE")
		values.Set("LAYERS", layer)
		values.Set("TIME", t)
		values.Set("WIDTH", strconv.Itoa(width))
		values.Set("HEIGHT", strconv.Itoa(height))
		values.Set("CRS", crs)
		values.Set("SRS", crs)
		values.Set("BBOX", bbox)

		canonical := canonicalNowcastQuery(values)
		if _, _, ok := readNowcastCache(canonical, nowcastCacheMaxAge("getmap", values)); !ok {
			cacheMiss++
			misses = append(misses, canonical)
		} else {
			cacheHit++
		}
		frames = append(frames, nowcastFrameInfo{
			URL:         "/api/nowcast/wms?" + canonical,
			Time:        t,
			Projection:  crs,
			ImageExtent: imageExtent,
		})
	}
	if len(misses) > 0 {
		warmQueued = len(misses)
		go warmNowcastCanonicals(misses)
	}

	out := struct {
		Layer       string             `json:"layer"`
		Projection  string             `json:"projection"`
		ImageExtent [4]float64         `json:"imageExtent"`
		FrameCount  int                `json:"frameCount"`
		Warming     bool               `json:"warming,omitempty"`
		Frames      []nowcastFrameInfo `json:"frames"`
	}{
		Layer:       layer,
		Projection:  crs,
		ImageExtent: imageExtent,
		FrameCount:  len(frames),
		Warming:     warmQueued > 0,
		Frames:      frames,
	}
	_ = json.NewEncoder(w).Encode(out)
	log.Printf(
		"[nowcast/frames] done: layer=%q frames=%d cache_hit=%d cache_miss=%d warm_queued=%d elapsed=%s",
		layer,
		len(frames),
		cacheHit,
		cacheMiss,
		warmQueued,
		time.Since(started).Round(time.Millisecond),
	)
}

func handleSatelliteFrames(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	w.Header().Set("Content-Type", "application/json")
	requestedLayer := strings.TrimSpace(r.URL.Query().Get("layer"))
	if requestedLayer == "" {
		requestedLayer = strings.TrimSpace(os.Getenv("EUMETVIEW_LAYER"))
	}
	if requestedLayer == "" {
		requestedLayer = "auto"
	}
	limit := parseQueryInt(r, "limit", 18, 1, 72)
	cadenceMin := parseQueryInt(r, "cadenceMin", 10, 5, 60)
	cadence := time.Duration(cadenceMin) * time.Minute
	log.Printf("[satellite/frames] request: layer=%q limit=%d cadence=%dmin", requestedLayer, limit, cadenceMin)
	token, err := getEUMETViewAccessToken(r.Context())
	authMode := "token"
	if err != nil {
		log.Printf("EUMET token unavailable, use public access mode: %v", err)
		token = ""
		authMode = "public"
	}
	_, capXML, err := getEUMETCapabilitiesWithFallback(r.Context(), token)
	if err != nil {
		http.Error(w, fmt.Sprintf("eumet capabilities error: %v", err), http.StatusBadGateway)
		log.Printf("[satellite/frames] error: capabilities: %v", err)
		return
	}
	layerTimes, layerTitles, _, err := parseNowcastCapabilities(capXML)
	if err != nil {
		http.Error(w, fmt.Sprintf("eumet capabilities parse error: %v", err), http.StatusBadGateway)
		log.Printf("[satellite/frames] error: capabilities parse: %v", err)
		return
	}
	layer, resolvedTitle, err := resolveEUMETLayer(requestedLayer, layerTimes, layerTitles)
	if err != nil {
		http.Error(w, fmt.Sprintf("eumet layer resolution error: %v", err), http.StatusBadRequest)
		log.Printf("[satellite/frames] error: layer resolve: %v", err)
		return
	}
	times := intersectLayerTimes(layer, layerTimes)
	if len(times) == 0 {
		http.Error(w, "no available times for selected EUMET layer", http.StatusBadRequest)
		log.Printf("[satellite/frames] no times for layer=%q", layer)
		return
	}

	state.mu.RLock()
	defaultBounds := state.geoBounds
	state.mu.RUnlock()
	// Шире РФ, чтобы спутниковый кадр покрывал регион, как в EUMETView.
	// Глобальный композит: полный охват мира без региональной обрезки.
	west := parseQueryFloatOrDefault(r, "west", -180)
	south := parseQueryFloatOrDefault(r, "south", -90)
	east := parseQueryFloatOrDefault(r, "east", 180)
	north := parseQueryFloatOrDefault(r, "north", 90)
	if !NumberIsFinite(west) || !NumberIsFinite(south) || !NumberIsFinite(east) || !NumberIsFinite(north) || east <= west || north <= south {
		west, south, east, north = defaultBounds[0], defaultBounds[1], defaultBounds[2], defaultBounds[3]
	}
	defW, defH := defaultSatelliteImageSize(west, south, east, north)
	width := parseQueryInt(r, "width", defW, 128, 8192)
	height := parseQueryInt(r, "height", defH, 128, 8192)

	picked, err := pickSatelliteTimes(times, limit, cadence)
	if err != nil {
		http.Error(w, fmt.Sprintf("satellite time selection error: %v", err), http.StatusBadGateway)
		log.Printf("[satellite/frames] error: pick times: %v", err)
		return
	}
	log.Printf(
		"[satellite/frames] resolved: layer=%q title=%q auth=%s times_available=%d picked=%d size=%dx%d",
		layer, resolvedTitle, authMode, len(times), len(picked), width, height,
	)
	frames := make([]satelliteFrameInfo, 0, len(picked))
	imageExtent := [4]float64{west, south, east, north}
	cacheHit := 0
	cacheMiss := 0
	warmQueued := 0
	sourceID := "token"
	if strings.TrimSpace(token) == "" {
		sourceID = "public"
	}
	baseUsed := preferredEUMETWMSBases()[0]
	warmTasks := make([]satelliteWarmTask, 0, len(picked))
	for _, tStr := range picked {
		cacheKey := "img:" + satelliteImageCanonicalCacheKey(baseUsed, sourceID, layer, tStr, imageExtent, width, height)
		if _, _, ok := readSatelliteCache(cacheKey, satelliteImageCacheMaxAge(tStr)); ok {
			cacheHit++
		} else {
			cacheMiss++
			warmTasks = append(warmTasks, satelliteWarmTask{
				layer:   layer,
				timeStr: tStr,
				bounds:  imageExtent,
				width:   width,
				height:  height,
				token:   token,
			})
		}
		frames = append(frames, satelliteFrameInfo{
			URL: "/api/satellite/image?layer=" + url.QueryEscape(layer) +
				"&time=" + url.QueryEscape(tStr) +
				"&west=" + url.QueryEscape(strconv.FormatFloat(west, 'f', 6, 64)) +
				"&south=" + url.QueryEscape(strconv.FormatFloat(south, 'f', 6, 64)) +
				"&east=" + url.QueryEscape(strconv.FormatFloat(east, 'f', 6, 64)) +
				"&north=" + url.QueryEscape(strconv.FormatFloat(north, 'f', 6, 64)) +
				"&width=" + url.QueryEscape(strconv.Itoa(width)) +
				"&height=" + url.QueryEscape(strconv.Itoa(height)),
			Time:        tStr,
			Projection:  "EPSG:4326",
			ImageExtent: imageExtent,
		})
	}
	if len(warmTasks) > 0 {
		warmQueued = len(warmTasks)
		go warmSatelliteTasks(warmTasks)
	}

	out := struct {
		Provider    string               `json:"provider"`
		AuthMode    string               `json:"authMode,omitempty"`
		Requested   string               `json:"requestedLayer,omitempty"`
		Layer       string               `json:"layer"`
		LayerTitle  string               `json:"layerTitle,omitempty"`
		Projection  string               `json:"projection"`
		ImageExtent [4]float64           `json:"imageExtent"`
		FrameCount  int                  `json:"frameCount"`
		Warming     bool                 `json:"warming,omitempty"`
		Frames      []satelliteFrameInfo `json:"frames"`
	}{
		Provider:    "eumetview",
		AuthMode:    authMode,
		Requested:   requestedLayer,
		Layer:       layer,
		LayerTitle:  resolvedTitle,
		Projection:  "EPSG:4326",
		ImageExtent: imageExtent,
		FrameCount:  len(frames),
		Warming:     warmQueued > 0,
		Frames:      frames,
	}
	_ = json.NewEncoder(w).Encode(out)
	log.Printf(
		"[satellite/frames] done: layer=%q frames=%d cache_hit=%d cache_miss=%d warm_queued=%d elapsed=%s",
		layer, len(frames), cacheHit, cacheMiss, warmQueued, time.Since(started).Round(time.Millisecond),
	)
}

func handleSatelliteImageProxy(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	layer := strings.TrimSpace(r.URL.Query().Get("layer"))
	if layer == "" {
		layer = strings.TrimSpace(os.Getenv("EUMETVIEW_LAYER"))
	}
	if layer == "" || strings.EqualFold(layer, "auto") {
		layer = eumetDefaultLayer
	}
	timeStr := strings.TrimSpace(r.URL.Query().Get("time"))
	if timeStr == "" {
		http.Error(w, "time is required", http.StatusBadRequest)
		log.Printf("[satellite/image] bad request: missing time")
		return
	}
	west := parseQueryFloatOrDefault(r, "west", -180)
	south := parseQueryFloatOrDefault(r, "south", -90)
	east := parseQueryFloatOrDefault(r, "east", 180)
	north := parseQueryFloatOrDefault(r, "north", 90)
	defW, defH := defaultSatelliteImageSize(west, south, east, north)
	width := parseQueryInt(r, "width", defW, 128, 8192)
	height := parseQueryInt(r, "height", defH, 128, 8192)
	log.Printf(
		"[satellite/image] request: layer=%q time=%s size=%dx%d bounds=[%.4f %.4f %.4f %.4f]",
		layer, timeStr, width, height, west, south, east, north,
	)
	token, err := getEUMETViewAccessToken(r.Context())
	if err != nil {
		log.Printf("EUMET token unavailable in image proxy, use public access mode: %v", err)
		token = ""
	}
	_, body, contentType, status, err := fetchSatelliteImageWithFallback(
		r.Context(),
		token,
		layer,
		timeStr,
		[4]float64{west, south, east, north},
		width,
		height,
		true,
	)
	if err != nil {
		http.Error(w, fmt.Sprintf("satellite upstream error: %v", err), http.StatusBadGateway)
		log.Printf("[satellite/image] error: %v", err)
		return
	}
	if status != http.StatusOK {
		if contentType == "" {
			contentType = "text/plain; charset=utf-8"
		}
		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(status)
		_, _ = w.Write(body)
		log.Printf("[satellite/image] upstream non-200: status=%d content-type=%q bytes=%d elapsed=%s", status, contentType, len(body), time.Since(started).Round(time.Millisecond))
		return
	}
	if contentType == "" {
		contentType = "image/png"
	}
	w.Header().Set("Content-Type", contentType)
	_, _ = w.Write(body)
	log.Printf("[satellite/image] done: status=200 content-type=%q bytes=%d elapsed=%s", contentType, len(body), time.Since(started).Round(time.Millisecond))
}

func syncExternalSources(reason string) {
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), syncRequestTimeout)
	defer cancel()
	log.Printf("[sync] start: reason=%s window=%d", reason, syncFrameLimit)

	_ = syncExternalSourcesWithContext(ctx)
	log.Printf("[sync] done: reason=%s elapsed=%s", reason, time.Since(started).Round(time.Millisecond))
}

func syncExternalSourcesWithContext(ctx context.Context) error {
	var errs []error
	if err := syncNowcastFrames(ctx); err != nil {
		log.Printf("[sync] nowcast error: %v", err)
		errs = append(errs, fmt.Errorf("nowcast: %w", err))
	}
	if err := syncSatelliteFrames(ctx); err != nil {
		log.Printf("[sync] satellite error: %v", err)
		errs = append(errs, fmt.Errorf("satellite: %w", err))
	}
	if len(errs) == 0 {
		return nil
	}
	if len(errs) == 1 {
		return errs[0]
	}
	return fmt.Errorf("%v; %v", errs[0], errs[1])
}

func syncNowcastFrames(ctx context.Context) error {
	capXML, err := getNowcastCapabilitiesCached(ctx)
	if err != nil {
		return fmt.Errorf("capabilities: %w", err)
	}
	layerTimes, _, _, err := parseNowcastCapabilities(capXML)
	if err != nil {
		return fmt.Errorf("capabilities parse: %w", err)
	}

	state.mu.RLock()
	defaultBounds := state.geoBounds
	defaultSize := state.imageSize
	state.mu.RUnlock()
	west, south, east, north := defaultBounds[0], defaultBounds[1], defaultBounds[2], defaultBounds[3]
	width := max(1, defaultSize[0])
	height := max(1, defaultSize[1])
	if width <= 1 {
		width = 1024
	}
	if height <= 1 {
		height = 768
	}

	crs := "EPSG:3857"
	version := "1.3.0"
	minX, minY := lonLatToWebMercator(west, south)
	maxX, maxY := lonLatToWebMercator(east, north)
	bbox := fmt.Sprintf("%.3f,%.3f,%.3f,%.3f", minX, minY, maxX, maxY)

	keep := map[string]struct{}{
		"REQUEST=GetCapabilities&SERVICE=WMS&VERSION=1.3.0": {},
	}
	fetched := 0
	layers := listNowcastPrefetchLayers(layerTimes)
	for _, layer := range layers {
		times := intersectLayerTimes(layer, layerTimes)
		if len(times) == 0 {
			continue
		}
		if len(times) > syncFrameLimit {
			times = times[len(times)-syncFrameLimit:]
		}
		for _, t := range times {
			values := url.Values{}
			values.Set("SERVICE", "WMS")
			values.Set("VERSION", version)
			values.Set("REQUEST", "GetMap")
			values.Set("FORMAT", "image/png")
			values.Set("TRANSPARENT", "TRUE")
			values.Set("LAYERS", layer)
			values.Set("TIME", t)
			values.Set("WIDTH", strconv.Itoa(width))
			values.Set("HEIGHT", strconv.Itoa(height))
			values.Set("CRS", crs)
			values.Set("SRS", crs)
			values.Set("BBOX", bbox)
			canonical := canonicalNowcastQuery(values)
			keep[canonical] = struct{}{}

			if _, _, ok := readNowcastCache(canonical, 0); ok {
				continue
			}
			if _, _, _, err := fetchNowcastAndMaybeCache(ctx, canonical, "getmap", true); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err
				}
				log.Printf("[sync/nowcast] prefetch warning layer=%s time=%s: %v", layer, t, err)
				continue
			}
			fetched++
		}
	}
	pruned, err := pruneNowcastCacheExcept(keep)
	if err != nil {
		return err
	}
	log.Printf("[sync/nowcast] done: layers=%d keep=%d fetched=%d pruned=%d", len(layers), len(keep), fetched, pruned)
	return nil
}

func syncSatelliteFrames(ctx context.Context) error {
	requestedLayer := "auto"
	token, err := getEUMETViewAccessToken(ctx)
	authMode := "token"
	if err != nil {
		token = ""
		authMode = "public"
	}
	_, capXML, err := getEUMETCapabilitiesWithFallback(ctx, token)
	if err != nil {
		return fmt.Errorf("capabilities: %w", err)
	}
	layerTimes, layerTitles, _, err := parseNowcastCapabilities(capXML)
	if err != nil {
		return fmt.Errorf("capabilities parse: %w", err)
	}
	layers := listSatellitePrefetchLayers(requestedLayer, layerTimes, layerTitles)
	if len(layers) == 0 {
		return errors.New("no satellite layers selected for prefetch")
	}
	satelliteFrameLimit := parseIntEnv("SATELLITE_SYNC_FRAME_LIMIT", syncFrameLimit, 1, 72)

	state.mu.RLock()
	defaultBounds := state.geoBounds
	state.mu.RUnlock()
	west, south, east, north := defaultBounds[0], defaultBounds[1], defaultBounds[2], defaultBounds[3]
	if !NumberIsFinite(west) || !NumberIsFinite(south) || !NumberIsFinite(east) || !NumberIsFinite(north) || east <= west || north <= south {
		west, south, east, north = -180, -90, 180, 90
	}
	width, height := defaultSatelliteImageSize(west, south, east, north)
	bounds := [4]float64{west, south, east, north}

	keep := make(map[string]struct{}, satelliteFrameLimit*len(layers))
	fetched := 0
	layerCount := 0
	sourceID := "token"
	if strings.TrimSpace(token) == "" {
		sourceID = "public"
	}
	for _, layer := range layers {
		times := intersectLayerTimes(layer, layerTimes)
		if len(times) == 0 {
			continue
		}
		picked, err := pickSatelliteTimes(times, satelliteFrameLimit, time.Duration(frameIntervalMin)*time.Minute)
		if err != nil || len(picked) == 0 {
			continue
		}
		layerCount++
		for _, tStr := range picked {
			base, _, _, status, err := fetchSatelliteImageWithFallback(ctx, token, layer, tStr, bounds, width, height, true)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err
				}
				log.Printf("[sync/satellite] prefetch warning layer=%s time=%s: %v", layer, tStr, err)
				continue
			}
			if status != http.StatusOK {
				log.Printf("[sync/satellite] upstream non-200 layer=%s time=%s status=%d", layer, tStr, status)
				continue
			}
			cacheKey := satelliteImageCanonicalCacheKey(base, sourceID, layer, tStr, bounds, width, height)
			keep["img:"+cacheKey] = struct{}{}
			fetched++
		}
	}
	pruned, err := pruneSatelliteImageCacheExcept(keep)
	if err != nil {
		return err
	}
	log.Printf(
		"[sync/satellite] done: auth=%s layers=%d keep=%d fetched=%d pruned=%d",
		authMode, layerCount, len(keep), fetched, pruned,
	)
	return nil
}

type satelliteWarmTask struct {
	layer   string
	timeStr string
	bounds  [4]float64
	width   int
	height  int
	token   string
}

func warmNowcastCanonicals(canonicals []string) {
	if len(canonicals) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, canonical := range canonicals {
		if strings.TrimSpace(canonical) == "" {
			continue
		}
		if _, _, ok := readNowcastCache(canonical, 0); ok {
			continue
		}
		if _, _, _, err := fetchNowcastAndMaybeCache(ctx, canonical, "getmap", true); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
		}
	}
}

func warmSatelliteTasks(tasks []satelliteWarmTask) {
	if len(tasks) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	for _, task := range tasks {
		_, _, _, _, err := fetchSatelliteImageWithFallback(
			ctx,
			task.token,
			task.layer,
			task.timeStr,
			task.bounds,
			task.width,
			task.height,
			true,
		)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			continue
		}
	}
}

func listNowcastPrefetchLayers(layerTimes map[string][]string) []string {
	seen := make(map[string]struct{}, len(nowcastSyncLayers))
	out := make([]string, 0, len(nowcastSyncLayers))
	for _, candidate := range nowcastSyncLayers {
		layer := strings.TrimSpace(candidate)
		if layer == "" {
			continue
		}
		if _, ok := seen[layer]; ok {
			continue
		}
		times := layerTimes[layer]
		if len(times) == 0 {
			continue
		}
		seen[layer] = struct{}{}
		out = append(out, layer)
	}
	sort.Strings(out)
	return out
}

func listSatellitePrefetchLayers(
	requestedLayer string,
	layerTimes map[string][]string,
	layerTitles map[string]string,
) []string {
	seen := make(map[string]struct{}, 16)
	add := func(layer string) {
		name := strings.TrimSpace(layer)
		if name == "" || len(intersectLayerTimes(name, layerTimes)) == 0 {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
	}
	if resolved, _, err := resolveEUMETLayer(requestedLayer, layerTimes, layerTitles); err == nil {
		add(resolved)
	}
	if extra := strings.TrimSpace(os.Getenv("EUMETVIEW_PREFETCH_LAYERS")); extra != "" {
		for _, token := range strings.Split(extra, ",") {
			add(token)
		}
	}
	// По умолчанию префетчим только реально используемый слой (+ явные override из env),
	// иначе startup может легко превысить timeout на десятках тяжелых спутниковых запросов.
	if parseBoolEnv("EUMETVIEW_PREFETCH_BROAD", false) {
		preferred := []string{
			strings.TrimSpace(os.Getenv("EUMETVIEW_LAYER")),
			"European HRV RGB 0 degree",
			eumetDefaultLayer,
			"msg_fes:rgb_eview",
			"msg_iodc:rgb_eview",
			"mumi:worldcloudmap_ir108",
			"msg_fes:ir108",
			"msg_iodc:ir108",
			"msg_fes:ir108_hrv",
			"Meteosat:msg_ir108",
		}
		for _, layer := range preferred {
			add(layer)
		}
		for name, times := range layerTimes {
			if len(times) == 0 {
				continue
			}
			title := strings.ToLower(strings.TrimSpace(layerTitles[name]))
			lowName := strings.ToLower(strings.TrimSpace(name))
			if strings.Contains(lowName, "ir108") || strings.Contains(lowName, "ir 10.8") ||
				strings.Contains(lowName, "brightness") || strings.Contains(title, "infrared") ||
				strings.Contains(title, "brightness temperature") {
				add(name)
			}
		}
	}
	out := make([]string, 0, len(seen))
	for layer := range seen {
		out = append(out, layer)
	}
	sort.Strings(out)
	return out
}

func parseBoolEnv(key string, def bool) bool {
	raw := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if raw == "" {
		return def
	}
	switch raw {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return def
	}
}

func parseIntEnv(key string, def int, min int, max int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func runBlockingWarmup() error {
	retries := parseIntEnv("BLOCKING_WARMUP_RETRIES", 2, 1, 10)
	timeoutSec := parseIntEnv("BLOCKING_WARMUP_TIMEOUT_SEC", int(syncRequestTimeout.Seconds()), 30, 1800)
	attemptSleepSec := parseIntEnv("BLOCKING_WARMUP_RETRY_SLEEP_SEC", 5, 1, 120)
	var lastErr error
	started := time.Now()
	for attempt := 1; attempt <= retries; attempt++ {
		attemptStarted := time.Now()
		log.Printf("[warmup] attempt %d/%d start", attempt, retries)
		fetchAndProcess()
		state.mu.RLock()
		hdReady := state.initialLoadDone && len(state.frames) > 0
		state.mu.RUnlock()
		if !hdReady {
			lastErr = errors.New("hd frames are not ready after pipeline run")
		} else {
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
			err := syncExternalSourcesWithContext(ctx)
			cancel()
			if err == nil {
				log.Printf("[warmup] complete: attempts=%d elapsed=%s", attempt, time.Since(started).Round(time.Millisecond))
				return nil
			}
			lastErr = err
		}
		log.Printf("[warmup] attempt %d failed after %s: %v", attempt, time.Since(attemptStarted).Round(time.Millisecond), lastErr)
		if attempt < retries {
			time.Sleep(time.Duration(attemptSleepSec) * time.Second)
		}
	}
	return fmt.Errorf("blocking warmup failed after %d attempts: %w", retries, lastErr)
}

func satelliteImageCacheMaxAge(timeStr string) time.Duration {
	if strings.TrimSpace(timeStr) == "" {
		return 8 * time.Minute
	}
	// Исторические кадры immutable.
	return 0
}

func preferredEUMETWMSBases() []string {
	var out []string
	add := func(s string) {
		v := strings.TrimSpace(s)
		if v == "" {
			return
		}
		for _, e := range out {
			if strings.EqualFold(e, v) {
				return
			}
		}
		out = append(out, v)
	}
	add(os.Getenv("EUMETVIEW_WMS_URL"))
	add(eumetviewWMSBase)
	if len(out) == 0 {
		out = append(out, "https://view.eumetsat.int/geoserver/ows")
	}
	return out
}

func getEUMETCapabilitiesWithFallback(ctx context.Context, token string) (string, []byte, error) {
	var lastErr error
	for _, base := range preferredEUMETWMSBases() {
		body, err := getEUMETCapabilitiesCached(ctx, base, token)
		if err == nil {
			return base, body, nil
		}
		lastErr = err
	}
	if lastErr != nil {
		return "", nil, lastErr
	}
	return "", nil, errors.New("no eumet wms endpoints configured")
}

func fetchSatelliteImageWithFallback(
	ctx context.Context,
	token string,
	layer string,
	timeStr string,
	bounds [4]float64,
	width int,
	height int,
	allowCacheWrite bool,
) (string, []byte, string, int, error) {
	var lastErr error
	var lastStatus int
	var lastBody []byte
	var lastContentType string
	for _, base := range preferredEUMETWMSBases() {
		body, contentType, status, err := fetchSatelliteImageAndMaybeCache(
			ctx,
			base,
			token,
			layer,
			timeStr,
			bounds,
			width,
			height,
			allowCacheWrite,
		)
		if err == nil {
			return base, body, contentType, status, nil
		}
		lastStatus = status
		lastBody = body
		lastContentType = contentType
		lastErr = err
	}
	if lastErr != nil {
		return "", lastBody, lastContentType, lastStatus, lastErr
	}
	return "", nil, "", 0, errors.New("no eumet wms endpoints configured")
}

func fetchSatelliteImageAndMaybeCache(
	ctx context.Context,
	wmsBase string,
	token string,
	layer string,
	timeStr string,
	bounds [4]float64,
	width int,
	height int,
	allowCacheWrite bool,
) ([]byte, string, int, error) {
	west, south, east, north := bounds[0], bounds[1], bounds[2], bounds[3]
	bbox := fmt.Sprintf("%.6f,%.6f,%.6f,%.6f", west, south, east, north)
	values := url.Values{}
	values.Set("SERVICE", "WMS")
	values.Set("VERSION", "1.3.0")
	values.Set("REQUEST", "GetMap")
	values.Set("LAYERS", layer)
	values.Set("STYLES", "")
	values.Set("FORMAT", "image/png")
	values.Set("TRANSPARENT", "TRUE")
	// CRS:84 исключает неоднозначность порядка осей (lon,lat).
	values.Set("CRS", "CRS:84")
	values.Set("SRS", "CRS:84")
	values.Set("BBOX", bbox)
	values.Set("WIDTH", strconv.Itoa(width))
	values.Set("HEIGHT", strconv.Itoa(height))
	values.Set("TIME", timeStr)
	sourceID := "token"
	if strings.TrimSpace(token) == "" {
		sourceID = "public"
	}
	cacheKey := sourceID + "|" + strings.TrimSpace(wmsBase) + "|" + canonicalNowcastQuery(values)
	cacheMaxAge := satelliteImageCacheMaxAge(timeStr)
	if body, contentType, ok := readSatelliteCache("img:"+cacheKey, cacheMaxAge); ok {
		if !isSatelliteImagePayload(contentType, body) {
			log.Printf("[satellite/cache] DROP invalid image cache: layer=%q time=%s source=%s content-type=%q bytes=%d", layer, timeStr, sourceID, contentType, len(body))
			removeCacheEntryByBase(satelliteCacheFileBase("img:" + cacheKey))
		} else {
			log.Printf("[satellite/cache] HIT image: layer=%q time=%s source=%s bytes=%d", layer, timeStr, sourceID, len(body))
			return body, contentType, http.StatusOK, nil
		}
	}
	log.Printf("[satellite/cache] MISS image: layer=%q time=%s source=%s -> fetch upstream", layer, timeStr, sourceID)
	upstreamValues := cloneValues(values)
	if strings.TrimSpace(token) != "" {
		upstreamValues.Set("access_token", token)
	}
	upstreamURL := strings.TrimSpace(wmsBase) + "?" + canonicalNowcastQuery(upstreamValues)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, upstreamURL, nil)
	if err != nil {
		return nil, "", 0, err
	}
	req.Header.Set("User-Agent", nowcastUserAgent)
	req.Header.Set("Accept", "image/png,*/*")

	resp, err := satelliteHTTP.Do(req)
	if err != nil {
		return nil, "", 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", 0, err
	}
	contentType := strings.TrimSpace(resp.Header.Get("Content-Type"))
	if contentType == "" {
		contentType = http.DetectContentType(body)
	}
	log.Printf("[satellite/upstream] image: layer=%q time=%s status=%d bytes=%d content-type=%q", layer, timeStr, resp.StatusCode, len(body), contentType)

	if resp.StatusCode != http.StatusOK {
		return body, contentType, resp.StatusCode, fmt.Errorf("satellite upstream status=%d body=%q", resp.StatusCode, compactPayloadSnippet(body, 240))
	}
	if !isSatelliteImagePayload(contentType, body) {
		return body, contentType, resp.StatusCode, fmt.Errorf(
			"satellite upstream non-image payload content-type=%q body=%q",
			contentType,
			compactPayloadSnippet(body, 240),
		)
	}
	if allowCacheWrite {
		writeSatelliteCache("img:"+cacheKey, contentType, body)
	}
	return body, contentType, resp.StatusCode, nil
}

func compactPayloadSnippet(body []byte, maxLen int) string {
	if len(body) == 0 {
		return ""
	}
	s := strings.TrimSpace(string(body))
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.Join(strings.Fields(s), " ")
	if maxLen <= 0 || len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

func isSatelliteImagePayload(contentType string, body []byte) bool {
	ct := strings.ToLower(strings.TrimSpace(contentType))
	if strings.HasPrefix(ct, "image/") {
		return true
	}
	sniff := strings.ToLower(strings.TrimSpace(string(body)))
	if strings.HasPrefix(sniff, "<?xml") {
		return false
	}
	if strings.HasPrefix(sniff, "<serviceexceptionreport") {
		return false
	}
	if strings.HasPrefix(sniff, "<ows:exceptionreport") {
		return false
	}
	return strings.HasPrefix(ct, "application/octet-stream") && len(body) > 0
}

func defaultSatelliteImageSize(west, south, east, north float64) (int, int) {
	lonSpan := math.Abs(east - west)
	latSpan := math.Abs(north - south)
	if !NumberIsFinite(lonSpan) || !NumberIsFinite(latSpan) || lonSpan <= 0 || latSpan <= 0 {
		return 3072, 1536
	}
	w := 3072
	h := int(math.Round(float64(w) * (latSpan / lonSpan)))
	if h < 512 {
		h = 512
	}
	if h > 8192 {
		h = 8192
	}
	return w, h
}

func fetchSatelliteAvailableCached(ctx context.Context, layer string) ([]satelliteAvailableEntry, error) {
	// Legacy stub: retained for backwards compatibility with previous architecture.
	// New architecture uses EUMETView WMS capabilities.
	_ = ctx
	_ = layer
	return nil, errors.New("met.no available API is not used in eumetview mode")
}

func getEUMETCapabilitiesCached(ctx context.Context, wmsBase string, token string) ([]byte, error) {
	values := url.Values{}
	values.Set("SERVICE", "WMS")
	values.Set("REQUEST", "GetCapabilities")
	values.Set("VERSION", "1.3.0")
	canonical := canonicalNowcastQuery(values)
	sourceID := "token"
	if strings.TrimSpace(token) == "" {
		sourceID = "public"
	}
	cacheKey := "capabilities:" + sourceID + "|" + strings.TrimSpace(wmsBase) + "|" + canonical
	if body, _, ok := readSatelliteCache(cacheKey, 10*time.Minute); ok {
		log.Printf("[satellite/cache] HIT capabilities: source=%s base=%s bytes=%d", sourceID, strings.TrimSpace(wmsBase), len(body))
		return body, nil
	}
	log.Printf("[satellite/cache] MISS capabilities: source=%s base=%s -> fetch upstream", sourceID, strings.TrimSpace(wmsBase))
	upstreamValues := cloneValues(values)
	if strings.TrimSpace(token) != "" {
		upstreamValues.Set("access_token", token)
	}
	upstreamURL := strings.TrimSpace(wmsBase) + "?" + canonicalNowcastQuery(upstreamValues)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, upstreamURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", nowcastUserAgent)
	req.Header.Set("Accept", "application/json,*/*")

	resp, err := satelliteHTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		log.Printf("[satellite/upstream] capabilities non-200: source=%s base=%s status=%d", sourceID, strings.TrimSpace(wmsBase), resp.StatusCode)
		return nil, fmt.Errorf("available http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	writeSatelliteCache(cacheKey, "application/xml", body)
	log.Printf("[satellite/upstream] capabilities done: source=%s base=%s bytes=%d", sourceID, strings.TrimSpace(wmsBase), len(body))
	return body, nil
}

func pickSatelliteTimes(all []string, limit int, cadence time.Duration) ([]string, error) {
	type stamp struct {
		raw string
		t   time.Time
	}
	parsed := make([]stamp, 0, len(all))
	for _, s := range all {
		t, err := parseSatelliteTime(s)
		if err != nil {
			continue
		}
		parsed = append(parsed, stamp{raw: s, t: t.UTC()})
	}
	if len(parsed) == 0 {
		return nil, errors.New("no parseable timestamps in capabilities")
	}
	sort.Slice(parsed, func(i, j int) bool { return parsed[i].t.Before(parsed[j].t) })
	pickedRev := make([]stamp, 0, limit)
	var lastPicked time.Time
	for i := len(parsed) - 1; i >= 0 && len(pickedRev) < limit; i-- {
		cur := parsed[i]
		if len(pickedRev) == 0 {
			pickedRev = append(pickedRev, cur)
			lastPicked = cur.t
			continue
		}
		if lastPicked.Sub(cur.t) >= cadence {
			pickedRev = append(pickedRev, cur)
			lastPicked = cur.t
		}
	}
	if len(pickedRev) < limit {
		seen := make(map[string]struct{}, len(pickedRev))
		for _, p := range pickedRev {
			seen[p.raw] = struct{}{}
		}
		for i := len(parsed) - 1; i >= 0 && len(pickedRev) < limit; i-- {
			if _, ok := seen[parsed[i].raw]; ok {
				continue
			}
			pickedRev = append(pickedRev, parsed[i])
			seen[parsed[i].raw] = struct{}{}
		}
	}
	out := make([]string, 0, len(pickedRev))
	for i := len(pickedRev) - 1; i >= 0; i-- {
		out = append(out, pickedRev[i].raw)
	}
	return out, nil
}

func resolveEUMETLayer(
	requested string,
	layerTimes map[string][]string,
	layerTitles map[string]string,
) (string, string, error) {
	norm := func(s string) string {
		s = strings.ToLower(strings.TrimSpace(s))
		s = strings.ReplaceAll(s, "_", " ")
		s = strings.ReplaceAll(s, ":", " ")
		s = strings.Join(strings.Fields(s), " ")
		return s
	}
	resolveByAlias := func(raw string) string {
		key := norm(raw)
		aliases := map[string][]string{
			"european hrv rgb 0 degree": {"msg_fes:rgb_eview", "msg_iodc:rgb_eview"},
			"european hrv rgb":          {"msg_fes:rgb_eview", "msg_iodc:rgb_eview"},
			"hrv rgb 0 degree":          {"msg_fes:rgb_eview", "msg_iodc:rgb_eview"},
			"hrv rgb":                   {"msg_fes:rgb_eview", "msg_iodc:rgb_eview"},
			"msg fes ir108 hrv":         {"msg_fes:ir108", "msg_iodc:ir108"},
			"msg_fes ir108 hrv":         {"msg_fes:ir108", "msg_iodc:ir108"},
			"msg_fes:ir108_hrv":         {"msg_fes:ir108", "msg_iodc:ir108"},
		}
		candidates := aliases[key]
		for _, candidate := range candidates {
			if len(intersectLayerTimes(candidate, layerTimes)) > 0 {
				return candidate
			}
		}
		return ""
	}
	req := strings.TrimSpace(requested)
	if req != "" && !strings.EqualFold(req, "auto") {
		if len(intersectLayerTimes(req, layerTimes)) > 0 {
			return req, strings.TrimSpace(layerTitles[req]), nil
		}
		if resolved := resolveByAlias(req); resolved != "" {
			return resolved, strings.TrimSpace(layerTitles[resolved]), nil
		}
		reqNorm := norm(req)
		for name, times := range layerTimes {
			if len(times) == 0 {
				continue
			}
			title := strings.TrimSpace(layerTitles[name])
			nameNorm := norm(name)
			titleNorm := norm(title)
			if reqNorm == nameNorm || reqNorm == titleNorm ||
				strings.Contains(nameNorm, reqNorm) || strings.Contains(titleNorm, reqNorm) {
				return name, title, nil
			}
		}
		return "", "", fmt.Errorf("layer %q has no available times (exact/fuzzy match failed)", req)
	}
	candidates := []string{
		strings.TrimSpace(os.Getenv("EUMETVIEW_LAYER")),
		"European HRV RGB 0 degree",
		eumetDefaultLayer,
		"msg_fes:rgb_eview",
		"msg_iodc:rgb_eview",
		"msg_fes:ir108_hrv",
		"msg_fes:ir108",
		"msg_iodc:ir108",
		"mumi:worldcloudmap_ir108",
		"Meteosat:msg_ir108",
	}
	if extra := strings.TrimSpace(os.Getenv("EUMETVIEW_LAYER_CANDIDATES")); extra != "" {
		for _, token := range strings.Split(extra, ",") {
			name := strings.TrimSpace(token)
			if name != "" {
				candidates = append([]string{name}, candidates...)
			}
		}
	}
	seen := make(map[string]struct{}, len(candidates))
	for _, c := range candidates {
		if c == "" {
			continue
		}
		if _, ok := seen[c]; ok {
			continue
		}
		seen[c] = struct{}{}
		if len(intersectLayerTimes(c, layerTimes)) > 0 {
			return c, strings.TrimSpace(layerTitles[c]), nil
		}
	}

	// Эвристика: выбираем первый слой с временной осью, максимально похожий на IR10.8.
	best := ""
	bestTitle := ""
	bestScore := -1
	for name, times := range layerTimes {
		if len(times) == 0 {
			continue
		}
		title := strings.TrimSpace(layerTitles[name])
		sn := strings.ToLower(name + " " + title)
		score := 0
		if strings.Contains(sn, "msg_fes") {
			score += 4
		}
		if strings.Contains(sn, "rgb_eview") || strings.Contains(sn, "european hrv rgb") {
			score += 14
		}
		if strings.Contains(sn, "hrv") || strings.Contains(sn, "rgb") {
			score += 8
		}
		if strings.Contains(sn, "ir108") || strings.Contains(sn, "ir 10.8") || strings.Contains(sn, "10.8") {
			score += 3
		}
		if strings.Contains(sn, "brightness") || strings.Contains(sn, "temperature") {
			score += 2
		}
		if strings.Contains(sn, "airmass") || strings.Contains(sn, "dust") || strings.Contains(sn, "ash") {
			score -= 2
		}
		if score > bestScore || (score == bestScore && name < best) {
			bestScore = score
			best = name
			bestTitle = title
		}
	}
	if best == "" {
		return "", "", errors.New("no WMS layers with available times")
	}
	return best, bestTitle, nil
}

func parseSatelliteTime(raw string) (time.Time, error) {
	s := strings.TrimSpace(raw)
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05Z",
	}
	var lastErr error
	for _, layout := range layouts {
		t, err := time.Parse(layout, s)
		if err == nil {
			return t, nil
		}
		lastErr = err
	}
	return time.Time{}, lastErr
}

func cloneValues(src url.Values) url.Values {
	dst := make(url.Values, len(src))
	for k, vals := range src {
		cp := make([]string, len(vals))
		copy(cp, vals)
		dst[k] = cp
	}
	return dst
}

func getEUMETViewAccessToken(ctx context.Context) (string, error) {
	if token := strings.TrimSpace(os.Getenv("EUMETVIEW_ACCESS_TOKEN")); token != "" {
		return token, nil
	}
	clientID := strings.TrimSpace(os.Getenv("EUMETSAT_CONSUMER_KEY"))
	clientSecret := strings.TrimSpace(os.Getenv("EUMETSAT_CONSUMER_SECRET"))
	if clientID == "" || clientSecret == "" {
		return "", errors.New("set EUMETVIEW_ACCESS_TOKEN or EUMETSAT_CONSUMER_KEY/EUMETSAT_CONSUMER_SECRET")
	}
	eumetTokenMu.Lock()
	defer eumetTokenMu.Unlock()
	if eumetAccessToken != "" && time.Until(eumetTokenExpiry) > 60*time.Second {
		return eumetAccessToken, nil
	}
	endpoints := []string{eumetTokenURL, eumetTokenURLAlt}
	if custom := strings.TrimSpace(os.Getenv("EUMETVIEW_TOKEN_URL")); custom != "" {
		endpoints = append([]string{custom}, endpoints...)
	}
	var lastErr error
	for _, endpoint := range endpoints {
		token, exp, err := requestEUMETAccessToken(ctx, endpoint, clientID, clientSecret)
		if err != nil {
			lastErr = err
			continue
		}
		eumetAccessToken = token
		eumetTokenExpiry = exp
		return eumetAccessToken, nil
	}
	if lastErr != nil {
		return "", lastErr
	}
	return "", errors.New("failed to obtain EUMETSAT token")
}

func getNowcastAccessToken(ctx context.Context) (string, error) {
	if token := strings.TrimSpace(os.Getenv("NOWCAST_ACCESS_TOKEN")); token != "" {
		return token, nil
	}
	nowcastTokenMu.Lock()
	defer nowcastTokenMu.Unlock()
	if nowcastAccessToken != "" && time.Until(nowcastTokenExpiry) > 60*time.Second {
		return nowcastAccessToken, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, nowcastTokenURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/json,*/*")
	req.Header.Set("Referer", nowcastReferer)
	req.Header.Set("User-Agent", nowcastUserAgent)
	resp, err := nowcastHTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("nowcast token http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var tr nowcastTokenResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		return "", err
	}
	token := strings.TrimSpace(tr.Token)
	if token == "" {
		return "", errors.New("empty nowcast access token")
	}
	exp := time.Now().Add(3 * time.Minute)
	if parsed, ok := parseJWTExpiry(token); ok {
		exp = parsed
	}
	nowcastAccessToken = token
	nowcastTokenExpiry = exp
	return nowcastAccessToken, nil
}

func parseJWTExpiry(token string) (time.Time, bool) {
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) < 2 {
		return time.Time{}, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, false
	}
	var claim struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claim); err != nil {
		return time.Time{}, false
	}
	if claim.Exp <= 0 {
		return time.Time{}, false
	}
	return time.Unix(claim.Exp, 0), true
}

func withNowcastAccessToken(ctx context.Context, values url.Values) (url.Values, error) {
	out := cloneValues(values)
	if strings.TrimSpace(firstNonEmpty(out.Get("token"), out.Get("TOKEN"))) != "" {
		return out, nil
	}
	token, err := getNowcastAccessToken(ctx)
	if err != nil {
		return nil, err
	}
	out.Set("token", token)
	return out, nil
}

func requestEUMETAccessToken(
	ctx context.Context,
	endpoint string,
	clientID string,
	clientSecret string,
) (string, time.Time, error) {
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", time.Time{}, err
	}
	auth := base64.StdEncoding.EncodeToString([]byte(clientID + ":" + clientSecret))
	req.Header.Set("Authorization", "Basic "+auth)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", nowcastUserAgent)
	resp, err := satelliteHTTP.Do(req)
	if err != nil {
		return "", time.Time{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", time.Time{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return "", time.Time{}, fmt.Errorf("token http %d (%s): %s", resp.StatusCode, endpoint, strings.TrimSpace(string(body)))
	}
	var tr eumetTokenResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		return "", time.Time{}, err
	}
	if strings.TrimSpace(tr.AccessToken) == "" {
		return "", time.Time{}, errors.New("empty access token from EUMETSAT gateway")
	}
	token := strings.TrimSpace(tr.AccessToken)
	exp := tr.ExpiresIn
	if exp <= 0 {
		exp = 3300
	}
	return token, time.Now().Add(time.Duration(exp) * time.Second), nil
}

func satelliteCacheFileBase(key string) string {
	sum := sha1.Sum([]byte(key))
	return filepath.Join(satelliteCacheDir, fmt.Sprintf("%x", sum[:]))
}

type cacheEntryMeta struct {
	Base        string
	Key         string
	ContentType string
	CreatedUnix int64
}

func readCacheEntryMeta(metaPath string) (cacheEntryMeta, bool) {
	raw, err := os.ReadFile(metaPath)
	if err != nil {
		return cacheEntryMeta{}, false
	}
	lines := strings.Split(string(raw), "\n")
	if len(lines) < 2 {
		return cacheEntryMeta{}, false
	}
	tUnix, err := strconv.ParseInt(strings.TrimSpace(lines[0]), 10, 64)
	if err != nil {
		return cacheEntryMeta{}, false
	}
	contentType := strings.TrimSpace(lines[1])
	key := ""
	if len(lines) >= 3 {
		key = strings.TrimSpace(lines[2])
	}
	base := strings.TrimSuffix(metaPath, ".meta")
	return cacheEntryMeta{
		Base:        base,
		Key:         key,
		ContentType: contentType,
		CreatedUnix: tUnix,
	}, true
}

func removeCacheEntryByBase(base string) {
	if strings.TrimSpace(base) == "" {
		return
	}
	_ = os.Remove(base + ".meta")
	_ = os.Remove(base + ".bin")
}

func readSatelliteCache(key string, maxAge time.Duration) ([]byte, string, bool) {
	base := satelliteCacheFileBase(key)
	metaPath := base + ".meta"
	bodyPath := base + ".bin"
	meta, ok := readCacheEntryMeta(metaPath)
	if !ok {
		return nil, "", false
	}
	if maxAge > 0 && time.Since(time.Unix(meta.CreatedUnix, 0)) > maxAge {
		return nil, "", false
	}
	body, err := os.ReadFile(bodyPath)
	if err != nil {
		return nil, "", false
	}
	contentType := meta.ContentType
	if contentType == "" {
		contentType = http.DetectContentType(body)
	}
	return body, contentType, true
}

func writeSatelliteCache(key string, contentType string, body []byte) {
	base := satelliteCacheFileBase(key)
	metaPath := base + ".meta"
	bodyPath := base + ".bin"
	_ = os.WriteFile(bodyPath, body, 0644)
	meta := fmt.Sprintf("%d\n%s\n%s", time.Now().Unix(), strings.TrimSpace(contentType), strings.TrimSpace(key))
	_ = os.WriteFile(metaPath, []byte(meta), 0644)
}

func satelliteImageCanonicalCacheKey(
	wmsBase string,
	sourceID string,
	layer string,
	timeStr string,
	bounds [4]float64,
	width int,
	height int,
) string {
	west, south, east, north := bounds[0], bounds[1], bounds[2], bounds[3]
	bbox := fmt.Sprintf("%.6f,%.6f,%.6f,%.6f", west, south, east, north)
	values := url.Values{}
	values.Set("SERVICE", "WMS")
	values.Set("VERSION", "1.3.0")
	values.Set("REQUEST", "GetMap")
	values.Set("LAYERS", layer)
	values.Set("STYLES", "")
	values.Set("FORMAT", "image/png")
	values.Set("TRANSPARENT", "TRUE")
	values.Set("CRS", "CRS:84")
	values.Set("SRS", "CRS:84")
	values.Set("BBOX", bbox)
	values.Set("WIDTH", strconv.Itoa(width))
	values.Set("HEIGHT", strconv.Itoa(height))
	values.Set("TIME", timeStr)
	return sourceID + "|" + strings.TrimSpace(wmsBase) + "|" + canonicalNowcastQuery(values)
}

func pruneSatelliteImageCacheExcept(keep map[string]struct{}) (int, error) {
	entries, err := os.ReadDir(satelliteCacheDir)
	if err != nil {
		return 0, err
	}
	pruned := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".meta") {
			continue
		}
		metaPath := filepath.Join(satelliteCacheDir, e.Name())
		meta, ok := readCacheEntryMeta(metaPath)
		if !ok {
			removeCacheEntryByBase(strings.TrimSuffix(metaPath, ".meta"))
			pruned++
			continue
		}
		key := strings.TrimSpace(meta.Key)
		// Старый формат мета без ключа считаем устаревшим.
		if key == "" {
			removeCacheEntryByBase(meta.Base)
			pruned++
			continue
		}
		// Чистим только img-кэш, capabilities/прочее оставляем.
		if !strings.HasPrefix(key, "img:") {
			continue
		}
		if _, ok := keep[key]; ok {
			continue
		}
		removeCacheEntryByBase(meta.Base)
		pruned++
	}
	return pruned, nil
}

func lonLatToWebMercator(lon, lat float64) (float64, float64) {
	const originShift = 20037508.342789244
	lat = math.Max(math.Min(lat, 85.05112878), -85.05112878)
	x := lon * originShift / 180.0
	y := math.Log(math.Tan((90.0+lat)*math.Pi/360.0)) / (math.Pi / 180.0)
	y = y * originShift / 180.0
	return x, y
}

func nowcastCacheMaxAge(reqType string, q url.Values) time.Duration {
	switch strings.ToLower(strings.TrimSpace(reqType)) {
	case "getcapabilities":
		return 10 * time.Minute
	case "getmap":
		timeVal := strings.TrimSpace(firstNonEmpty(q.Get("TIME"), q.Get("time")))
		if timeVal == "" || timeVal == "-1" {
			return 2 * time.Minute
		}
		// Исторические кадры считаем immutable:
		// один раз скачали, дальше используем бессрочно.
		return 0
	default:
		return 2 * time.Minute
	}
}

func fetchNowcastAndMaybeCache(
	ctx context.Context,
	canonical string,
	reqType string,
	allowCacheWrite bool,
) ([]byte, string, int, error) {
	values, err := url.ParseQuery(canonical)
	if err != nil {
		return nil, "", 0, err
	}
	upstreamValues, err := withNowcastAccessToken(ctx, values)
	if err != nil {
		return nil, "", 0, err
	}
	upstreamURL := nowcastWMSBaseURL + "?" + canonicalNowcastQuery(upstreamValues)
	reqURL, _ := url.Parse(upstreamURL)
	layer := ""
	timeVal := ""
	if reqURL != nil {
		layer = strings.TrimSpace(firstNonEmpty(reqURL.Query().Get("LAYERS"), reqURL.Query().Get("layers")))
		timeVal = strings.TrimSpace(firstNonEmpty(reqURL.Query().Get("TIME"), reqURL.Query().Get("time")))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, upstreamURL, nil)
	if err != nil {
		return nil, "", 0, err
	}
	req.Header.Set("Referer", nowcastReferer)
	req.Header.Set("User-Agent", nowcastUserAgent)
	req.Header.Set("Accept", "image/png,application/xml,text/xml,*/*")

	resp, err := nowcastHTTP.Do(req)
	if err != nil {
		return nil, "", 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", 0, err
	}
	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = http.DetectContentType(body)
	}
	if allowCacheWrite && resp.StatusCode == http.StatusOK && (reqType == "getmap" || reqType == "getcapabilities") {
		writeNowcastCache(canonical, contentType, body)
	}
	log.Printf("[nowcast/upstream] %s: status=%d bytes=%d layer=%q time=%q", strings.ToUpper(reqType), resp.StatusCode, len(body), layer, timeVal)
	return body, contentType, resp.StatusCode, nil
}

func handleNowcastWMSProxy(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	q := r.URL.Query()
	if q.Get("SERVICE") == "" && q.Get("service") == "" {
		q.Set("SERVICE", "WMS")
	}
	if q.Get("REQUEST") == "" && q.Get("request") == "" {
		q.Set("REQUEST", "GetMap")
	}
	if q.Get("VERSION") == "" && q.Get("version") == "" {
		q.Set("VERSION", "1.3.0")
	}

	reqType := strings.ToLower(strings.TrimSpace(firstNonEmpty(q.Get("REQUEST"), q.Get("request"))))
	layer := strings.TrimSpace(firstNonEmpty(q.Get("LAYERS"), q.Get("layers")))
	timeVal := strings.TrimSpace(firstNonEmpty(q.Get("TIME"), q.Get("time")))
	canonical := canonicalNowcastQuery(q)
	cacheMaxAge := nowcastCacheMaxAge(reqType, q)
	if (reqType == "getmap" || reqType == "getcapabilities") && cacheMaxAge > 0 {
		if body, contentType, ok := readNowcastCache(canonical, cacheMaxAge); ok {
			w.Header().Set("Content-Type", contentType)
			w.Header().Set("X-Nowcast-Cache", "HIT")
			_, _ = w.Write(body)
			log.Printf("[nowcast/proxy] HIT: type=%s layer=%q time=%q bytes=%d elapsed=%s", reqType, layer, timeVal, len(body), time.Since(started).Round(time.Millisecond))
			return
		}
	}

	body, contentType, status, err := fetchNowcastAndMaybeCache(r.Context(), canonical, reqType, true)
	if err != nil {
		http.Error(w, fmt.Sprintf("nowcast upstream error: %v", err), http.StatusBadGateway)
		log.Printf("[nowcast/proxy] error: type=%s layer=%q time=%q err=%v", reqType, layer, timeVal, err)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("X-Nowcast-Cache", "MISS")
	w.WriteHeader(status)
	_, _ = w.Write(body)
	log.Printf("[nowcast/proxy] MISS: type=%s layer=%q time=%q status=%d bytes=%d elapsed=%s", reqType, layer, timeVal, status, len(body), time.Since(started).Round(time.Millisecond))
}

func parseNowcastCapabilities(xmlData []byte) (map[string][]string, map[string]string, []nowcastLayerInfo, error) {
	var caps wmsCapabilities
	if err := xml.Unmarshal(xmlData, &caps); err != nil {
		return nil, nil, nil, err
	}
	layerTimes := make(map[string][]string)
	layerTitles := make(map[string]string)
	var infos []nowcastLayerInfo
	var walk func(l wmsLayer)
	walk = func(l wmsLayer) {
		name := strings.TrimSpace(l.Name)
		if name != "" {
			times := extractLayerTimes(l)
			if len(times) > 0 {
				layerTimes[name] = times
			}
			layerTitles[name] = strings.TrimSpace(l.Title)
			infos = append(infos, nowcastLayerInfo{
				Name:      name,
				Title:     strings.TrimSpace(l.Title),
				TimeCount: len(times),
			})
		}
		for _, child := range l.Layers {
			walk(child)
		}
	}
	walk(caps.Capability.Layer)
	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	return layerTimes, layerTitles, infos, nil
}

func extractLayerTimes(l wmsLayer) []string {
	raw := ""
	for _, d := range l.Dimensions {
		if strings.EqualFold(strings.TrimSpace(d.Name), "time") {
			raw = strings.TrimSpace(d.Text)
			break
		}
	}
	if raw == "" {
		for _, d := range l.Extents {
			if strings.EqualFold(strings.TrimSpace(d.Name), "time") {
				raw = strings.TrimSpace(d.Text)
				break
			}
		}
	}
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{})
	for _, p := range parts {
		s := strings.TrimSpace(p)
		if s == "" {
			continue
		}
		if strings.Contains(s, "/") {
			expanded := expandWMSTimeIntervalToken(s, 400)
			for _, e := range expanded {
				if _, ok := seen[e]; ok {
					continue
				}
				seen[e] = struct{}{}
				out = append(out, e)
			}
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

func expandWMSTimeIntervalToken(token string, hardLimit int) []string {
	parts := strings.Split(token, "/")
	if len(parts) != 3 {
		return nil
	}
	start, err1 := parseSatelliteTime(parts[0])
	end, err2 := parseSatelliteTime(parts[1])
	step, err3 := parseISODurationSimple(parts[2])
	if err1 != nil || err2 != nil || err3 != nil || step <= 0 || end.Before(start) {
		return nil
	}
	// Важно: берём метки с конца интервала (самые свежие),
	// иначе при лимите можно зафиксироваться на старых датах.
	outRev := make([]string, 0, 64)
	for t := end.UTC(); !t.Before(start.UTC()); t = t.Add(-step) {
		outRev = append(outRev, t.Format(time.RFC3339))
		if hardLimit > 0 && len(outRev) >= hardLimit {
			break
		}
	}
	out := make([]string, 0, len(outRev))
	for i := len(outRev) - 1; i >= 0; i-- {
		out = append(out, outRev[i])
	}
	return out
}

func parseISODurationSimple(raw string) (time.Duration, error) {
	s := strings.ToUpper(strings.TrimSpace(raw))
	if !strings.HasPrefix(s, "PT") {
		return 0, fmt.Errorf("unsupported duration format: %s", raw)
	}
	s = strings.TrimPrefix(s, "PT")
	// Поддержка PT<n>H / PT<n>M / PT<n>S.
	if strings.HasSuffix(s, "H") {
		n, err := strconv.Atoi(strings.TrimSuffix(s, "H"))
		if err != nil || n <= 0 {
			return 0, fmt.Errorf("invalid hour duration: %s", raw)
		}
		return time.Duration(n) * time.Hour, nil
	}
	if strings.HasSuffix(s, "M") {
		n, err := strconv.Atoi(strings.TrimSuffix(s, "M"))
		if err != nil || n <= 0 {
			return 0, fmt.Errorf("invalid minute duration: %s", raw)
		}
		return time.Duration(n) * time.Minute, nil
	}
	if strings.HasSuffix(s, "S") {
		n, err := strconv.Atoi(strings.TrimSuffix(s, "S"))
		if err != nil || n <= 0 {
			return 0, fmt.Errorf("invalid second duration: %s", raw)
		}
		return time.Duration(n) * time.Second, nil
	}
	return 0, fmt.Errorf("unsupported duration token: %s", raw)
}

func intersectLayerTimes(layersCSV string, layerTimes map[string][]string) []string {
	var layers []string
	for _, token := range strings.Split(layersCSV, ",") {
		name := strings.TrimSpace(token)
		if name != "" {
			layers = append(layers, name)
		}
	}
	if len(layers) == 0 {
		return nil
	}
	base := layerTimes[layers[0]]
	if len(base) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(base))
	for _, t := range base {
		set[t] = struct{}{}
	}
	for _, name := range layers[1:] {
		times := layerTimes[name]
		if len(times) == 0 {
			return nil
		}
		nextSet := make(map[string]struct{}, len(times))
		for _, t := range times {
			if _, ok := set[t]; ok {
				nextSet[t] = struct{}{}
			}
		}
		set = nextSet
		if len(set) == 0 {
			return nil
		}
	}
	out := make([]string, 0, len(set))
	for t := range set {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

func getNowcastCapabilitiesCached(ctx context.Context) ([]byte, error) {
	q := "REQUEST=GetCapabilities&SERVICE=WMS&VERSION=1.3.0"
	if body, _, ok := readNowcastCache(q, 10*time.Minute); ok {
		log.Printf("[nowcast/cache] HIT capabilities: bytes=%d", len(body))
		return body, nil
	}
	log.Printf("[nowcast/cache] MISS capabilities -> fetch upstream")
	values, err := url.ParseQuery(q)
	if err != nil {
		return nil, err
	}
	upstreamValues, err := withNowcastAccessToken(ctx, values)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, nowcastWMSBaseURL+"?"+canonicalNowcastQuery(upstreamValues), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Referer", nowcastReferer)
	req.Header.Set("User-Agent", nowcastUserAgent)
	req.Header.Set("Accept", "application/xml,text/xml,*/*")
	resp, err := nowcastHTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("[nowcast/upstream] capabilities non-200: status=%d", resp.StatusCode)
		return nil, fmt.Errorf("capabilities http %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/xml"
	}
	writeNowcastCache(q, contentType, body)
	log.Printf("[nowcast/upstream] capabilities done: bytes=%d content-type=%q", len(body), contentType)
	return body, nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func canonicalNowcastQuery(q map[string][]string) string {
	clean := make(map[string][]string, len(q))
	for k, vals := range q {
		key := strings.TrimSpace(k)
		if key == "" {
			continue
		}
		if strings.EqualFold(key, "_") || strings.EqualFold(key, "cache_bust") {
			continue
		}
		clean[key] = vals
	}
	keys := make([]string, 0, len(clean))
	for k := range clean {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return strings.ToLower(keys[i]) < strings.ToLower(keys[j]) })
	var parts []string
	for _, k := range keys {
		vals := clean[k]
		sort.Strings(vals)
		for _, v := range vals {
			parts = append(parts, fmt.Sprintf("%s=%s", url.QueryEscape(k), url.QueryEscape(v)))
		}
	}
	return strings.Join(parts, "&")
}

func nowcastCacheFileBase(query string) string {
	sum := sha1.Sum([]byte(query))
	return filepath.Join(nowcastCacheDir, fmt.Sprintf("%x", sum[:]))
}

func readNowcastCache(query string, maxAge time.Duration) ([]byte, string, bool) {
	base := nowcastCacheFileBase(query)
	metaPath := base + ".meta"
	bodyPath := base + ".bin"
	meta, ok := readCacheEntryMeta(metaPath)
	if !ok {
		return nil, "", false
	}
	// maxAge <= 0 => бессрочный кэш (без проверки возраста).
	if maxAge > 0 && time.Since(time.Unix(meta.CreatedUnix, 0)) > maxAge {
		return nil, "", false
	}
	body, err := os.ReadFile(bodyPath)
	if err != nil {
		return nil, "", false
	}
	contentType := meta.ContentType
	if contentType == "" {
		contentType = http.DetectContentType(body)
	}
	return body, contentType, true
}

func writeNowcastCache(query string, contentType string, body []byte) {
	base := nowcastCacheFileBase(query)
	metaPath := base + ".meta"
	bodyPath := base + ".bin"
	_ = os.WriteFile(bodyPath, body, 0644)
	meta := fmt.Sprintf("%d\n%s\n%s", time.Now().Unix(), strings.TrimSpace(contentType), strings.TrimSpace(query))
	_ = os.WriteFile(metaPath, []byte(meta), 0644)
}

func pruneNowcastCacheExcept(keep map[string]struct{}) (int, error) {
	entries, err := os.ReadDir(nowcastCacheDir)
	if err != nil {
		return 0, err
	}
	pruned := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".meta") {
			continue
		}
		metaPath := filepath.Join(nowcastCacheDir, e.Name())
		meta, ok := readCacheEntryMeta(metaPath)
		if !ok {
			removeCacheEntryByBase(strings.TrimSuffix(metaPath, ".meta"))
			pruned++
			continue
		}
		key := strings.TrimSpace(meta.Key)
		if key == "" {
			removeCacheEntryByBase(meta.Base)
			pruned++
			continue
		}
		if _, ok := keep[key]; ok {
			continue
		}
		removeCacheEntryByBase(meta.Base)
		pruned++
	}
	return pruned, nil
}

func fetchAndProcess() {
	jobStarted := time.Now()
	log.Printf("[pipeline] fetch start: url=%s", radarURL)

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Get(radarURL)
	if err != nil {
		log.Printf("[pipeline] fetch error: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("[pipeline] fetch HTTP %d", resp.StatusCode)
		return
	}
	log.Printf(
		"[pipeline] fetch response: status=%s content-type=%q content-length=%d",
		resp.Status,
		resp.Header.Get("Content-Type"),
		resp.ContentLength,
	)

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("[pipeline] read body error: %v", err)
		return
	}
	log.Printf("[pipeline] download complete: bytes=%d elapsed=%s", len(data), time.Since(jobStarted).Round(time.Millisecond))

	decodeStarted := time.Now()
	g, err := gif.DecodeAll(bytes.NewReader(data))
	if err != nil {
		log.Printf("[pipeline] GIF decode error: %v", err)
		return
	}
	log.Printf(
		"[pipeline] GIF decoded: frames=%d canvas=%dx%d elapsed=%s",
		len(g.Image),
		g.Config.Width,
		g.Config.Height,
		time.Since(decodeStarted).Round(time.Millisecond),
	)

	assetsStarted := time.Now()
	ref, err := loadReference()
	if err != nil {
		log.Printf("[pipeline] reference load: %v (processing without subtraction)", err)
	} else {
		log.Printf("[pipeline] reference loaded")
	}

	mask, err := loadMask()
	if err != nil {
		log.Printf("[pipeline] mask load: %v (processing without mask)", err)
	} else {
		log.Printf("[pipeline] mask loaded")
	}

	cityMask, err := loadCityMask()
	if err != nil {
		log.Printf("[pipeline] city mask load: %v (processing without city mask)", err)
	} else {
		log.Printf("[pipeline] city mask loaded")
	}
	log.Printf("[pipeline] assets ready: elapsed=%s", time.Since(assetsStarted).Round(time.Millisecond))

	state.mu.RLock()
	latestOnly := state.initialLoadDone
	state.mu.RUnlock()
	log.Printf("[pipeline] process start: latestOnly=%t", latestOnly)

	processStarted := time.Now()
	frames := processGIF(g, ref, mask, cityMask, latestOnly)
	if len(frames) == 0 {
		log.Printf("[pipeline] no frames produced")
		return
	}
	log.Printf("[pipeline] process done: output_frames=%d elapsed=%s", len(frames), time.Since(processStarted).Round(time.Millisecond))

	state.mu.Lock()
	state.frames = frames
	state.imageSize = [2]int{g.Config.Width, g.Config.Height}
	state.initialLoadDone = true
	state.lastUpdate = time.Now()
	state.mu.Unlock()

	log.Printf("[pipeline] state updated: frames=%d canvas=%dx%d", len(frames), g.Config.Width, g.Config.Height)
	log.Printf("[pipeline] cycle done: total_elapsed=%s", time.Since(jobStarted).Round(time.Millisecond))
}

func NumberIsFinite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

func loadReference() (image.Image, error) {
	// Пути относительно каталога server/ (см. константу referenceFile = ../reference_map.png)
	pngPath := referenceFile
	jpgPath := filepath.Join(filepath.Dir(referenceFile), "reference_map.jpg")
	for _, path := range []string{pngPath, jpgPath} {
		f, err := os.Open(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		img, _, decErr := image.Decode(f)
		_ = f.Close()
		if decErr != nil {
			return nil, fmt.Errorf("%s: %w", path, decErr)
		}
		return img, nil
	}
	return nil, fmt.Errorf("эталон не найден (искали %s и %s)", pngPath, jpgPath)
}

func loadMask() (image.Image, error) {
	f, err := os.Open(maskFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return png.Decode(f)
}

func loadCityMask() (image.Image, error) {
	f, err := os.Open(cityMaskFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return png.Decode(f)
}

func initThinSplineRuntime() error {
	gdalInfoCmd, err := resolveExternalCommand("gdalinfo")
	if err != nil {
		return err
	}
	gdalTranslateCmd, err := resolveExternalCommand("gdal_translate")
	if err != nil {
		return err
	}
	gdalWarpCmd, err := resolveExternalCommand("gdalwarp")
	if err != nil {
		return err
	}
	if _, err := os.Stat(pointsFile); err != nil {
		return fmt.Errorf("points file missing: %s", pointsFile)
	}
	if _, err := os.Stat(referenceGeo); err != nil {
		return fmt.Errorf("reference geotiff missing: %s", referenceGeo)
	}

	gcps, err := readGCPArgs(pointsFile)
	if err != nil {
		return err
	}
	if q, qErr := summarizeGCPQuality(pointsFile); qErr != nil {
		log.Printf("GCP quality check warning: %v", qErr)
	} else {
		log.Printf(
			"GCP quality: enabled=%d span=(%.1f x %.1f) residual(mean=%.4f max=%.4f samples=%d)",
			q.EnabledCount, q.SourceSpanX, q.SourceSpanY, q.ResidualMean, q.ResidualMax, q.ResidualSampleSize,
		)
		if q.EnabledCount < 10 {
			log.Printf("GCP quality alert: very few enabled points (%d)", q.EnabledCount)
		}
		if q.SourceSpanX < 450 || q.SourceSpanY < 450 {
			log.Printf("GCP quality alert: point spread is narrow (spanX=%.1f spanY=%.1f)", q.SourceSpanX, q.SourceSpanY)
		}
		if q.ResidualSampleSize > 0 && q.ResidualMax > 1.5 {
			log.Printf("GCP quality alert: high residual max=%.3f", q.ResidualMax)
		}
	}
	grid, err := readReferenceGrid(gdalInfoCmd, referenceGeo)
	if err != nil {
		return err
	}

	thinSplineState = thinSplineRuntime{
		GCPArgs:          gcps,
		GridW:            grid.GridW,
		GridH:            grid.GridH,
		MinX:             grid.MinX,
		MinY:             grid.MinY,
		MaxX:             grid.MaxX,
		MaxY:             grid.MaxY,
		GDALInfoCmd:      gdalInfoCmd,
		GDALTranslateCmd: gdalTranslateCmd,
		GDALWarpCmd:      gdalWarpCmd,
	}
	state.mu.Lock()
	state.geoBounds = [4]float64{grid.MinX, grid.MinY, grid.MaxX, grid.MaxY}
	state.mu.Unlock()
	return nil
}

func resolveExternalCommand(cmd string) (string, error) {
	if path, err := exec.LookPath(cmd); err == nil {
		return path, nil
	}
	exeName := cmd + ".exe"
	for _, dir := range candidateGDALBinDirs() {
		if strings.TrimSpace(dir) == "" {
			continue
		}
		full := filepath.Join(dir, exeName)
		if st, err := os.Stat(full); err == nil && !st.IsDir() {
			return full, nil
		}
	}
	return "", fmt.Errorf("%s not found; set PATH or GDAL_BIN_DIR", cmd)
}

func candidateGDALBinDirs() []string {
	seen := make(map[string]struct{})
	var dirs []string
	add := func(v string) {
		v = strings.TrimSpace(v)
		if v == "" {
			return
		}
		if _, ok := seen[v]; ok {
			return
		}
		seen[v] = struct{}{}
		dirs = append(dirs, v)
	}

	add(os.Getenv("GDAL_BIN_DIR"))
	if qgisPrefix := strings.TrimSpace(os.Getenv("QGIS_PREFIX_PATH")); qgisPrefix != "" {
		add(filepath.Join(qgisPrefix, "bin"))
		add(qgisPrefix)
	}
	add(`C:\OSGeo4W\bin`)
	add(`C:\OSGeo4W64\bin`)

	for _, base := range []string{`C:\Program Files`, `C:\Program Files (x86)`} {
		entries, err := os.ReadDir(base)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			name := strings.ToLower(e.Name())
			if strings.HasPrefix(name, "qgis") {
				add(filepath.Join(base, e.Name(), "bin"))
			}
		}
	}
	return dirs
}

func readGCPArgs(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var args []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "mapX,") {
			continue
		}
		rec := strings.Split(line, ",")
		if len(rec) < 5 {
			continue
		}
		if strings.TrimSpace(rec[4]) != "1" {
			continue
		}
		mapX, err := strconv.ParseFloat(strings.TrimSpace(rec[0]), 64)
		if err != nil {
			continue
		}
		mapY, err := strconv.ParseFloat(strings.TrimSpace(rec[1]), 64)
		if err != nil {
			continue
		}
		srcX, err := strconv.ParseFloat(strings.TrimSpace(rec[2]), 64)
		if err != nil {
			continue
		}
		srcY, err := strconv.ParseFloat(strings.TrimSpace(rec[3]), 64)
		if err != nil {
			continue
		}
		// QGIS *.points хранит sourceY отрицательным (ось вверх); для GDAL pixel/line line должен быть вниз.
		srcY = -srcY
		args = append(args,
			"-gcp", fmt.Sprintf("%.15g", srcX), fmt.Sprintf("%.15g", srcY),
			fmt.Sprintf("%.15g", mapX), fmt.Sprintf("%.15g", mapY),
		)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read points file: %w", err)
	}
	if len(args) < 15 {
		return nil, fmt.Errorf("not enough enabled GCP points")
	}
	return args, nil
}

func summarizeGCPQuality(path string) (gcpQualityReport, error) {
	f, err := os.Open(path)
	if err != nil {
		return gcpQualityReport{}, err
	}
	defer f.Close()

	var out gcpQualityReport
	out.SourceMinX = math.MaxFloat64
	out.SourceMinY = math.MaxFloat64
	out.SourceMaxX = -math.MaxFloat64
	out.SourceMaxY = -math.MaxFloat64

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "mapX,") {
			continue
		}
		rec := strings.Split(line, ",")
		if len(rec) < 5 {
			continue
		}
		if strings.TrimSpace(rec[4]) != "1" {
			continue
		}
		srcX, errX := strconv.ParseFloat(strings.TrimSpace(rec[2]), 64)
		srcY, errY := strconv.ParseFloat(strings.TrimSpace(rec[3]), 64)
		if errX != nil || errY != nil {
			continue
		}
		out.EnabledCount++
		if srcX < out.SourceMinX {
			out.SourceMinX = srcX
		}
		if srcY < out.SourceMinY {
			out.SourceMinY = srcY
		}
		if srcX > out.SourceMaxX {
			out.SourceMaxX = srcX
		}
		if srcY > out.SourceMaxY {
			out.SourceMaxY = srcY
		}
		if len(rec) >= 8 {
			if residual, rErr := strconv.ParseFloat(strings.TrimSpace(rec[7]), 64); rErr == nil && NumberIsFinite(residual) {
				out.ResidualSampleSize++
				out.ResidualMean += residual
				if residual > out.ResidualMax {
					out.ResidualMax = residual
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return gcpQualityReport{}, fmt.Errorf("read points for quality check: %w", err)
	}
	if out.EnabledCount == 0 {
		return gcpQualityReport{}, fmt.Errorf("no enabled points for quality check")
	}
	out.SourceSpanX = out.SourceMaxX - out.SourceMinX
	out.SourceSpanY = out.SourceMaxY - out.SourceMinY
	if out.ResidualSampleSize > 0 {
		out.ResidualMean /= float64(out.ResidualSampleSize)
	}
	return out, nil
}

type gdalInfoJSON struct {
	Size              []int `json:"size"`
	CornerCoordinates struct {
		UpperLeft  []float64 `json:"upperLeft"`
		LowerRight []float64 `json:"lowerRight"`
	} `json:"cornerCoordinates"`
}

type refGrid struct {
	GridW int
	GridH int
	MinX  float64
	MinY  float64
	MaxX  float64
	MaxY  float64
}

func readReferenceGrid(gdalInfoCmd, path string) (refGrid, error) {
	cmd := exec.Command(gdalInfoCmd, "-json", path)
	out, err := cmd.Output()
	if err != nil {
		return refGrid{}, fmt.Errorf("gdalinfo failed: %w", err)
	}
	var info gdalInfoJSON
	if err := json.Unmarshal(out, &info); err != nil {
		return refGrid{}, fmt.Errorf("gdalinfo json decode failed: %w", err)
	}
	if len(info.Size) < 2 || len(info.CornerCoordinates.UpperLeft) < 2 || len(info.CornerCoordinates.LowerRight) < 2 {
		return refGrid{}, fmt.Errorf("invalid gdalinfo json fields")
	}
	minX := info.CornerCoordinates.UpperLeft[0]
	maxY := info.CornerCoordinates.UpperLeft[1]
	maxX := info.CornerCoordinates.LowerRight[0]
	minY := info.CornerCoordinates.LowerRight[1]
	return refGrid{
		GridW: info.Size[0],
		GridH: info.Size[1],
		MinX:  minX,
		MinY:  minY,
		MaxX:  maxX,
		MaxY:  maxY,
	}, nil
}

func warpFrameThinSpline(srcPNGPath, dstPNGPath string) error {
	// Страхуемся от запуска из другого cwd/последующей очистки временной папки:
	// директории для промежуточного и итогового файла должны существовать всегда.
	if err := os.MkdirAll(tmpWarpDir, 0755); err != nil {
		return fmt.Errorf("cannot create tmp warp dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(dstPNGPath), 0755); err != nil {
		return fmt.Errorf("cannot create dst warp dir: %w", err)
	}

	name := strings.TrimSuffix(filepath.Base(srcPNGPath), filepath.Ext(srcPNGPath))
	tmpPath := filepath.Join(tmpWarpDir, name+"_gcp.tif")

	argsTranslate := append([]string{"-of", "GTiff", "-a_srs", "EPSG:4326"}, thinSplineState.GCPArgs...)
	argsTranslate = append(argsTranslate, srcPNGPath, tmpPath)
	if out, err := exec.Command(thinSplineState.GDALTranslateCmd, argsTranslate...).CombinedOutput(); err != nil {
		return fmt.Errorf("gdal_translate failed: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	defer os.Remove(tmpPath)

	argsWarp := []string{
		"-overwrite",
		"-tps",
		"-r", "near",
		"-dstalpha",
		"-of", "PNG",
		"-t_srs", "EPSG:4326",
		"-te",
		fmt.Sprintf("%.15g", thinSplineState.MinX),
		fmt.Sprintf("%.15g", thinSplineState.MinY),
		fmt.Sprintf("%.15g", thinSplineState.MaxX),
		fmt.Sprintf("%.15g", thinSplineState.MaxY),
		"-ts",
		strconv.Itoa(thinSplineState.GridW),
		strconv.Itoa(thinSplineState.GridH),
		tmpPath,
		dstPNGPath,
	}
	if out, err := exec.Command(thinSplineState.GDALWarpCmd, argsWarp...).CombinedOutput(); err != nil {
		return fmt.Errorf("gdalwarp failed: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func detectRadarsFromMask() ([]radarSite, error) {
	if !thinSplineEnabled {
		return nil, fmt.Errorf("thin-spline runtime is disabled")
	}
	maskWarpPath := filepath.Join(warpedDir, warpedMaskFileName)
	if err := warpFrameThinSpline(maskFile, maskWarpPath); err != nil {
		return nil, err
	}

	f, err := os.Open(maskWarpPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	img, err := png.Decode(f)
	if err != nil {
		return nil, err
	}
	b := img.Bounds()
	w, h := b.Dx(), b.Dy()
	if w < 3 || h < 3 {
		return nil, fmt.Errorf("warped mask is too small")
	}

	black := make([]bool, w*h)
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			r, g, b2, a := img.At(x+b.Min.X, y+b.Min.Y).RGBA()
			if a == 0 {
				black[y*w+x] = false
				continue
			}
			avg := (r + g + b2) / 3
			black[y*w+x] = avg < 0x7fff
		}
	}

	dist := make([]float64, w*h)
	const inf = 1e9
	for i := range dist {
		if black[i] {
			dist[i] = inf
		} else {
			dist[i] = 0
		}
	}

	diag := math.Sqrt2
	// Forward pass
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			i := y*w + x
			v := dist[i]
			if x > 0 && dist[i-1]+1 < v {
				v = dist[i-1] + 1
			}
			if y > 0 && dist[i-w]+1 < v {
				v = dist[i-w] + 1
			}
			if x > 0 && y > 0 && dist[i-w-1]+diag < v {
				v = dist[i-w-1] + diag
			}
			if x+1 < w && y > 0 && dist[i-w+1]+diag < v {
				v = dist[i-w+1] + diag
			}
			dist[i] = v
		}
	}
	// Backward pass
	for y := h - 1; y >= 0; y-- {
		for x := w - 1; x >= 0; x-- {
			i := y*w + x
			v := dist[i]
			if x+1 < w && dist[i+1]+1 < v {
				v = dist[i+1] + 1
			}
			if y+1 < h && dist[i+w]+1 < v {
				v = dist[i+w] + 1
			}
			if x+1 < w && y+1 < h && dist[i+w+1]+diag < v {
				v = dist[i+w+1] + diag
			}
			if x > 0 && y+1 < h && dist[i+w-1]+diag < v {
				v = dist[i+w-1] + diag
			}
			dist[i] = v
		}
	}

	// Local maxima in distance field.
	candidates := make([]radarCandidate, 0, 128)
	minRadiusPx := 12.0
	for y := 1; y < h-1; y++ {
		for x := 1; x < w-1; x++ {
			i := y*w + x
			if !black[i] {
				continue
			}
			d := dist[i]
			if d < minRadiusPx {
				continue
			}
			isMax := true
			for oy := -1; oy <= 1 && isMax; oy++ {
				for ox := -1; ox <= 1; ox++ {
					if ox == 0 && oy == 0 {
						continue
					}
					ni := (y+oy)*w + (x + ox)
					if dist[ni] > d {
						isMax = false
						break
					}
				}
			}
			if isMax {
				candidates = append(candidates, radarCandidate{x: x, y: y, r: d})
			}
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].r > candidates[j].r })

	// Non-maximum suppression to split overlapping circles.
	minSepPx := 55.0
	picked := make([]radarCandidate, 0, 64)
	for _, c := range candidates {
		ok := true
		for _, p := range picked {
			dx := float64(c.x - p.x)
			dy := float64(c.y - p.y)
			if dx*dx+dy*dy < minSepPx*minSepPx {
				ok = false
				break
			}
		}
		if ok {
			picked = append(picked, c)
		}
		if len(picked) >= 64 {
			break
		}
	}

	if len(picked) == 0 {
		return nil, fmt.Errorf("no radar candidates found from mask")
	}

	// Pixel -> geo (output grid is regular in EPSG:4326 by design).
	lonStep := (thinSplineState.MaxX - thinSplineState.MinX) / float64(max(1, w-1))
	latStep := (thinSplineState.MaxY - thinSplineState.MinY) / float64(max(1, h-1))

	sites := make([]radarSite, 0, len(picked))
	for _, c := range picked {
		lon := thinSplineState.MinX + float64(c.x)*lonStep
		lat := thinSplineState.MaxY - float64(c.y)*latStep
		latMetersPerDeg := 110540.0
		lonMetersPerDeg := 111320.0 * math.Cos(lat*math.Pi/180.0)
		rx := c.r * lonStep * lonMetersPerDeg
		ry := c.r * latStep * latMetersPerDeg
		rMeters := math.Sqrt(rx*rx + ry*ry)
		rKm := rMeters / 1000.0
		if rKm < 10 {
			continue
		}
		// Ограничиваем нереалистичные выбросы из-за слияний маски.
		if rKm > 550 {
			rKm = 550
		}
		sites = append(sites, radarSite{
			Lon:      lon,
			Lat:      lat,
			RadiusKm: rKm,
		})
	}

	return sites, nil
}

func loadManualRadars(auto []radarSite) ([]radarSite, error) {
	data, err := os.ReadFile(manualRadarsFile)
	if err != nil {
		return nil, err
	}
	var manual []radarSite
	if err := json.Unmarshal(data, &manual); err != nil {
		return nil, fmt.Errorf("parse %s: %w", manualRadarsFile, err)
	}
	if len(manual) == 0 {
		return nil, nil
	}

	// Если радиус не задан, берем у ближайшего авто-радара или дефолт.
	for i := range manual {
		if manual[i].RadiusKm > 0 {
			continue
		}
		manual[i].RadiusKm = nearestAutoRadiusKm(manual[i].Lon, manual[i].Lat, auto)
	}
	return manual, nil
}

func nearestAutoRadiusKm(lon, lat float64, auto []radarSite) float64 {
	if len(auto) == 0 {
		return 300 // безопасный дефолт
	}
	best := auto[0]
	bestD := math.MaxFloat64
	for _, a := range auto {
		dLon := (lon - a.Lon) * math.Cos(lat*math.Pi/180.0)
		dLat := lat - a.Lat
		d2 := dLon*dLon + dLat*dLat
		if d2 < bestD {
			bestD = d2
			best = a
		}
	}
	if best.RadiusKm <= 0 {
		return 300
	}
	return best.RadiusKm
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

const (
	motionBlockSize      = 16    // block size for city zone infill (subtractReference)
	cityMaskDark         = 20000 // avg < this => city (dark/black in mask). Cities are DARK in city_mask.png
	cleanHistorySatMerge = 20    // merge pixel into cleanHistory if saturation above this
	debugCityRed         = false // set true: paint city zones bright red to verify mask
	lightningDilatePx    = 2     // more aggressive: remove full plus + antialias halo
	keepLightningPluses  = true  // for HD layer: keep '+' glyphs, do not remove/infill them
)

var phenomenaPalette = [...]color.RGBA{
	{R: 156, G: 170, B: 179, A: 255}, // Облака в ср. яр.
	{R: 162, G: 198, B: 254, A: 255}, // Слоистая обл.
	{R: 70, G: 254, B: 149, A: 255},  // Осадки: слабые
	{R: 1, G: 194, B: 94, A: 255},    // Осадки: умеренные
	{R: 1, G: 154, B: 8, A: 255},     // Осадки: сильные
	{R: 255, G: 255, B: 131, A: 255}, // Кучевая облачность
	{R: 62, G: 137, B: 253, A: 255},  // Ливневые: слабые
	{R: 1, G: 58, B: 255, A: 255},    // Ливневые: умеренные
	{R: 2, G: 8, B: 119, A: 255},     // Ливневые: сильные
	{R: 255, G: 171, B: 128, A: 255}, // Гроза: (R)
	{R: 255, G: 89, B: 132, A: 255},  // Гроза: R
	{R: 253, G: 6, B: 9, A: 255},     // Гроза: R+
	{R: 205, G: 105, B: 8, A: 255},   // Град: слабый
	{R: 143, G: 73, B: 15, A: 255},   // Град: умеренный
	{R: 88, G: 14, B: 8, A: 255},     // Град: сильный
	{R: 255, G: 171, B: 255, A: 255}, // Гроза+Шквал: слабый
	{R: 255, G: 88, B: 255, A: 255},  // Гроза+Шквал: умеренный
	{R: 200, G: 9, B: 202, A: 255},   // Гроза+Шквал: сильный
	{R: 47, G: 49, B: 73, A: 255},    // Смерч
}

var lightningGlyphPalette = [...]color.RGBA{
	{R: 247, G: 231, B: 18, A: 255}, // yellow
	{R: 18, G: 18, B: 18, A: 255},   // black
	{R: 240, G: 22, B: 22, A: 255},  // red
	{R: 241, G: 140, B: 30, A: 255}, // orange
	{R: 225, G: 25, B: 236, A: 255}, // magenta
}

func nearestPhenomenaColor(c color.RGBA) color.RGBA {
	best := phenomenaPalette[0]
	bestD := int(^uint(0) >> 1)
	for _, p := range phenomenaPalette {
		dr := int(c.R) - int(p.R)
		dg := int(c.G) - int(p.G)
		db := int(c.B) - int(p.B)
		d := dr*dr + dg*dg + db*db
		if d < bestD {
			bestD = d
			best = p
		}
	}
	return best
}

func nearestPhenomenaColorWithDistance(c color.RGBA) (color.RGBA, int, int) {
	best := phenomenaPalette[0]
	bestD := int(^uint(0) >> 1)
	secondD := int(^uint(0) >> 1)
	for _, p := range phenomenaPalette {
		dr := int(c.R) - int(p.R)
		dg := int(c.G) - int(p.G)
		db := int(c.B) - int(p.B)
		d := dr*dr + dg*dg + db*db
		if d < bestD {
			secondD = bestD
			bestD = d
			best = p
		} else if d < secondD {
			secondD = d
		}
	}
	if secondD == int(^uint(0)>>1) {
		secondD = bestD
	}
	return best, bestD, secondD
}

func mapToLegendColorStrict(c color.RGBA) (color.RGBA, bool) {
	best, bestD, secondD := nearestPhenomenaColorWithDistance(c)
	// Strict gate: only colors confidently close to a legend class.
	if bestD > 34*34 {
		return color.RGBA{}, false
	}
	if (secondD - bestD) < 120 {
		return color.RGBA{}, false
	}
	// "Смерч" is very dark and easy to trigger from map labels/noise.
	// Keep only very confident matches for this class.
	tornado := phenomenaPalette[len(phenomenaPalette)-1]
	if best == tornado {
		if bestD > 12*12 || (secondD-bestD) < 220 {
			return color.RGBA{}, false
		}
	}
	return color.RGBA{R: best.R, G: best.G, B: best.B, A: 255}, true
}

func mapToLegendColorInfill(c color.RGBA) (color.RGBA, bool) {
	best, bestD, secondD := nearestPhenomenaColorWithDistance(c)
	// Relaxed gate for city-label nowcast donors (still blocks random noise).
	if bestD > 52*52 {
		return color.RGBA{}, false
	}
	if (secondD - bestD) < 40 {
		return color.RGBA{}, false
	}
	legend := color.RGBA{R: best.R, G: best.G, B: best.B, A: 255}
	if isTornadoClass(legend) {
		return color.RGBA{}, false
	}
	return legend, true
}

func isTornadoClass(c color.RGBA) bool {
	tornado := phenomenaPalette[len(phenomenaPalette)-1]
	return c == tornado
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func smoothstep(edge0, edge1, x float64) float64 {
	if edge1 <= edge0 {
		if x >= edge1 {
			return 1
		}
		return 0
	}
	t := clamp01((x - edge0) / (edge1 - edge0))
	return t * t * (3 - 2*t)
}

func maxUint32(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}

func maxInt3(a, b, c int) int {
	m := a
	if b > m {
		m = b
	}
	if c > m {
		m = c
	}
	return m
}

func minInt3(a, b, c int) int {
	m := a
	if b < m {
		m = b
	}
	if c < m {
		m = c
	}
	return m
}

func isLightningGlyphColor(c color.RGBA) bool {
	// Fast-path rules for bright yellow/orange pluses and dark outline pixels.
	if c.R >= 200 && c.G >= 140 && c.B <= 120 {
		return true
	}
	if c.R >= 180 && c.G >= 170 && c.B <= 80 {
		return true
	}
	if c.R <= 45 && c.G <= 45 && c.B <= 45 {
		return true
	}

	dist := func(a, b color.RGBA) int {
		dr := int(a.R) - int(b.R)
		dg := int(a.G) - int(b.G)
		db := int(a.B) - int(b.B)
		return dr*dr + dg*dg + db*db
	}
	best := int(^uint(0) >> 1)
	bestIdx := -1
	for i, p := range lightningGlyphPalette {
		d := dist(c, p)
		if d < best {
			best = d
			bestIdx = i
		}
	}
	if bestIdx < 0 {
		return false
	}
	// Slightly stricter for black to avoid accidental dark-map picks.
	if bestIdx == 1 {
		return best <= 38*38
	}
	return best <= 48*48
}

func isCarrierPrecipColor(c color.RGBA) bool {
	best, bestD, _ := nearestPhenomenaColorWithDistance(c)
	if bestD > 40*40 {
		return false
	}
	// Осадки/ливневые классы: используем как признак "ядра" вокруг опасных пикселей.
	return best == phenomenaPalette[2] || // Осадки: слабые
		best == phenomenaPalette[3] || // Осадки: умеренные
		best == phenomenaPalette[4] || // Осадки: сильные
		best == phenomenaPalette[6] || // Ливневые: слабые
		best == phenomenaPalette[7] || // Ливневые: умеренные
		best == phenomenaPalette[8] // Ливневые: сильные
}

func isConvectiveCoreColor(c color.RGBA) bool {
	best, bestD, _ := nearestPhenomenaColorWithDistance(c)
	if bestD > 46*46 {
		return false
	}
	return best == phenomenaPalette[9] || // Гроза: (R)
		best == phenomenaPalette[10] || // Гроза: R
		best == phenomenaPalette[11] || // Гроза: R+
		best == phenomenaPalette[12] || // Град: слабый
		best == phenomenaPalette[13] || // Град: умеренный
		best == phenomenaPalette[14] || // Град: сильный
		best == phenomenaPalette[15] || // Гроза+Шквал: слабый
		best == phenomenaPalette[16] || // Гроза+Шквал: умеренный
		best == phenomenaPalette[17] // Гроза+Шквал: сильный
}

func legendClassIndex(c color.RGBA) (int, bool) {
	best, bestD, _ := nearestPhenomenaColorWithDistance(c)
	if bestD > 46*46 {
		return -1, false
	}
	for i, p := range phenomenaPalette {
		if p == best {
			return i, true
		}
	}
	return -1, false
}

func isHailLikeClass(idx int) bool {
	return idx == 12 || idx == 13 || idx == 14
}

func isThunderLikeClass(idx int) bool {
	return idx == 9 || idx == 10 || idx == 11 || idx == 15 || idx == 16 || idx == 17
}

func hasCarrierPrecipSupport(frame image.Image, bounds image.Rectangle, x, y int) bool {
	w, h := bounds.Dx(), bounds.Dy()
	support := 0
	samples := 0
	for oy := -2; oy <= 2; oy++ {
		for ox := -2; ox <= 2; ox++ {
			if ox == 0 && oy == 0 {
				continue
			}
			nx := x + ox
			ny := y + oy
			if nx < 0 || nx >= w || ny < 0 || ny >= h {
				continue
			}
			r, g, b, a := frame.At(nx+bounds.Min.X, ny+bounds.Min.Y).RGBA()
			if a == 0 {
				continue
			}
			samples++
			cc := color.RGBA{R: uint8(r >> 8), G: uint8(g >> 8), B: uint8(b >> 8), A: uint8(a >> 8)}
			if isCarrierPrecipColor(cc) {
				support++
				if support >= 4 {
					return true
				}
			}
		}
	}
	if samples == 0 {
		return false
	}
	return float64(support)/float64(samples) >= 0.28
}

func hasTemporalNeighborSignal(frame image.Image, bounds image.Rectangle, x, y int) bool {
	if frame == nil {
		return false
	}
	w, h := bounds.Dx(), bounds.Dy()
	for oy := -2; oy <= 2; oy++ {
		for ox := -2; ox <= 2; ox++ {
			nx := x + ox
			ny := y + oy
			if nx < 0 || nx >= w || ny < 0 || ny >= h {
				continue
			}
			r, g, b, a := frame.At(nx+bounds.Min.X, ny+bounds.Min.Y).RGBA()
			if a == 0 {
				continue
			}
			cc := color.RGBA{R: uint8(r >> 8), G: uint8(g >> 8), B: uint8(b >> 8), A: uint8(a >> 8)}
			if isLightningGlyphColor(cc) || isCarrierPrecipColor(cc) || isConvectiveCoreColor(cc) {
				return true
			}
		}
	}
	return false
}

func dilateBoolMask(mask []bool, w, h, radius int) {
	if mask == nil || radius <= 0 {
		return
	}
	out := make([]bool, len(mask))
	copy(out, mask)
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			if !mask[y*w+x] {
				continue
			}
			for oy := -radius; oy <= radius; oy++ {
				yy := y + oy
				if yy < 0 || yy >= h {
					continue
				}
				for ox := -radius; ox <= radius; ox++ {
					xx := x + ox
					if xx < 0 || xx >= w {
						continue
					}
					out[yy*w+xx] = true
				}
			}
		}
	}
	copy(mask, out)
}

func detectLightningGlyphMask(frame image.Image, prevFrame image.Image, nextFrame image.Image, bounds image.Rectangle) []bool {
	w, h := bounds.Dx(), bounds.Dy()
	if w <= 0 || h <= 0 {
		return nil
	}
	candidates := make([]bool, w*h)
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			r, g, b, a := frame.At(x+bounds.Min.X, y+bounds.Min.Y).RGBA()
			if a == 0 {
				continue
			}
			c := color.RGBA{R: uint8(r >> 8), G: uint8(g >> 8), B: uint8(b >> 8), A: uint8(a >> 8)}
			if isLightningGlyphColor(c) {
				candidates[y*w+x] = true
			}
		}
	}

	visited := make([]bool, w*h)
	mask := make([]bool, w*h)
	queue := make([]int, 0, 128)

	for idx := 0; idx < len(candidates); idx++ {
		if !candidates[idx] || visited[idx] {
			continue
		}
		queue = queue[:0]
		queue = append(queue, idx)
		visited[idx] = true
		component := make([]int, 0, 64)
		minX, maxX := w, 0
		minY, maxY := h, 0

		for q := 0; q < len(queue); q++ {
			p := queue[q]
			component = append(component, p)
			x := p % w
			y := p / w
			if x < minX {
				minX = x
			}
			if x > maxX {
				maxX = x
			}
			if y < minY {
				minY = y
			}
			if y > maxY {
				maxY = y
			}

			if x > 0 {
				n := p - 1
				if candidates[n] && !visited[n] {
					visited[n] = true
					queue = append(queue, n)
				}
			}
			if x+1 < w {
				n := p + 1
				if candidates[n] && !visited[n] {
					visited[n] = true
					queue = append(queue, n)
				}
			}
			if y > 0 {
				n := p - w
				if candidates[n] && !visited[n] {
					visited[n] = true
					queue = append(queue, n)
				}
			}
			if y+1 < h {
				n := p + w
				if candidates[n] && !visited[n] {
					visited[n] = true
					queue = append(queue, n)
				}
			}
		}

		area := len(component)
		bw := maxX - minX + 1
		bh := maxY - minY + 1
		if area < 5 || area > 520 || bw < 2 || bh < 2 || bw > 36 || bh > 36 {
			continue
		}
		fill := float64(area) / float64(max(1, bw*bh))
		if fill < 0.08 || fill > 0.90 {
			continue
		}
		// Plus glyph should be roughly compact and cross-like.
		if bw > bh*3 || bh > bw*3 {
			continue
		}
		cx := (minX + maxX) / 2
		cy := (minY + maxY) / 2
		rowHits := 0
		colHits := 0
		for _, p := range component {
			x := p % w
			y := p / w
			if absInt(y-cy) <= 1 {
				rowHits++
			}
			if absInt(x-cx) <= 1 {
				colHits++
			}
		}
		if rowHits < 3 || colHits < 3 {
			continue
		}
		armRight, armLeft, armDown, armUp := 0, 0, 0, 0
		carrierSupportHits := 0
		convectiveHits := 0
		temporalPersistHits := 0
		for _, p := range component {
			x := p % w
			y := p / w
			if absInt(y-cy) <= 1 {
				if x > cx {
					armRight++
				} else if x < cx {
					armLeft++
				}
			}
			if absInt(x-cx) <= 1 {
				if y > cy {
					armDown++
				} else if y < cy {
					armUp++
				}
			}
			if hasCarrierPrecipSupport(frame, bounds, x, y) {
				carrierSupportHits++
			}
			r, g, b, a := frame.At(x+bounds.Min.X, y+bounds.Min.Y).RGBA()
			if a > 0 {
				cc := color.RGBA{R: uint8(r >> 8), G: uint8(g >> 8), B: uint8(b >> 8), A: uint8(a >> 8)}
				if isConvectiveCoreColor(cc) {
					convectiveHits++
				}
			}
			if hasTemporalNeighborSignal(prevFrame, bounds, x, y) || hasTemporalNeighborSignal(nextFrame, bounds, x, y) {
				temporalPersistHits++
			}
		}
		if armRight < 2 || armLeft < 2 || armDown < 2 || armUp < 2 {
			continue
		}
		// Keep only distinctly cross-like components.
		armsSum := armRight + armLeft + armDown + armUp
		if armsSum*2 < area {
			continue
		}
		isTemporalPersistent := temporalPersistHits >= max(2, area/6)
		hasConvectiveNeighborhood := convectiveHits >= max(2, area/8)
		carrierRatio := float64(carrierSupportHits) / float64(max(1, area))
		isIsolated := carrierRatio < 0.22

		// Плюсы обычно одноразовые. Если сигнал держится во времени
		// и лежит в конвективном окружении, это скорее метео-ядро.
		if isTemporalPersistent && hasConvectiveNeighborhood && !isIsolated {
			continue
		}
		// Компактные конвективные ядра внутри осадков не режем.
		if !isIsolated && hasConvectiveNeighborhood && area <= 40 {
			continue
		}

		for _, p := range component {
			mask[p] = true
		}
	}

	dilateBoolMask(mask, w, h, lightningDilatePx)
	return mask
}

func buildCityInfillMask(cityMask image.Image, bounds image.Rectangle) []bool {
	if cityMask == nil {
		return nil
	}
	w, h := bounds.Dx(), bounds.Dy()
	out := make([]bool, w*h)
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			if isCityPixel(cityMask, x, y) {
				out[y*w+x] = true
			}
		}
	}
	return out
}

func maskHasPixel(mask []bool, w, h, x, y int) bool {
	if mask == nil {
		return false
	}
	if x < 0 || x >= w || y < 0 || y >= h {
		return false
	}
	return mask[y*w+x]
}

func mergeInfillMasks(baseMask, addMask []bool) []bool {
	if baseMask == nil && addMask == nil {
		return nil
	}
	if baseMask == nil {
		out := make([]bool, len(addMask))
		copy(out, addMask)
		return out
	}
	if addMask == nil {
		out := make([]bool, len(baseMask))
		copy(out, baseMask)
		return out
	}
	out := make([]bool, len(baseMask))
	copy(out, baseMask)
	for i := range out {
		out[i] = out[i] || addMask[i]
	}
	return out
}

func sampleMotionDonor(
	source *image.RGBA,
	vectors map[image.Point]image.Point,
	sourceArtifactMask []bool,
	x int,
	y int,
	w int,
	h int,
	satThreshold uint8,
) (color.RGBA, int, bool) {
	if source == nil {
		return color.RGBA{}, 0, false
	}
	pastX, pastY := x, y
	blockPt := image.Pt((x/motionBlockSize)*motionBlockSize, (y/motionBlockSize)*motionBlockSize)
	if vectors != nil {
		if v, ok := vectors[blockPt]; ok {
			pastX = x - v.X
			pastY = y - v.Y
		}
	}
	if pastX < 0 || pastX >= w || pastY < 0 || pastY >= h {
		return color.RGBA{}, 0, false
	}
	if maskHasPixel(sourceArtifactMask, w, h, pastX, pastY) {
		return color.RGBA{}, 0, false
	}
	pc := source.RGBAAt(pastX, pastY)
	if !isUsefulPrecipPixel(pc, satThreshold) {
		return color.RGBA{}, 0, false
	}
	legendColor, ok := mapToLegendColorInfill(color.RGBA{R: pc.R, G: pc.G, B: pc.B, A: 255})
	if !ok {
		return color.RGBA{}, 0, false
	}

	// Local support to avoid spreading isolated noise.
	support := 0
	for oy := -1; oy <= 1; oy++ {
		for ox := -1; ox <= 1; ox++ {
			nx := pastX + ox
			ny := pastY + oy
			if nx < 0 || nx >= w || ny < 0 || ny >= h {
				continue
			}
			if maskHasPixel(sourceArtifactMask, w, h, nx, ny) {
				continue
			}
			if isUsefulPrecipPixel(source.RGBAAt(nx, ny), satThreshold) {
				support++
			}
		}
	}
	if support < 1 {
		return color.RGBA{}, 0, false
	}
	return legendColor, support, true
}

func blockHasInfillPixels(infillMask []bool, x0, y0, blockW, blockH int, bounds image.Rectangle) bool {
	if infillMask == nil {
		return false
	}
	w := bounds.Dx()
	x1 := minInt(w, x0+blockW)
	y1 := minInt(bounds.Dy(), y0+blockH)
	for y := y0; y < y1; y++ {
		row := y * w
		for x := x0; x < x1; x++ {
			if infillMask[row+x] {
				return true
			}
		}
	}
	return false
}

func compareMotionWindow(curr, prev *image.RGBA, infillMask []bool, x0, y0, x1, y1, vx, vy int) (float64, int) {
	if curr == nil || prev == nil {
		return math.Inf(1), 0
	}
	b := curr.Bounds()
	w := b.Dx()
	step := 2
	var score int64
	samples := 0
	for y := y0; y < y1; y += step {
		for x := x0; x < x1; x += step {
			if infillMask != nil && infillMask[y*w+x] {
				continue
			}
			c := curr.RGBAAt(x+b.Min.X, y+b.Min.Y)
			if c.A == 0 {
				continue
			}
			px := x - vx
			py := y - vy
			if px < 0 || px >= b.Dx() || py < 0 || py >= b.Dy() {
				continue
			}
			p := prev.RGBAAt(px+b.Min.X, py+b.Min.Y)
			if p.A == 0 {
				continue
			}
			score += int64(absInt(int(c.R)-int(p.R)) + absInt(int(c.G)-int(p.G)) + absInt(int(c.B)-int(p.B)))
			samples++
		}
	}
	if samples == 0 {
		return math.Inf(1), 0
	}
	return float64(score) / float64(samples), samples
}

func estimateGlobalMotionVector(curr, prev *image.RGBA, infillMask []bool, searchRadius int) (image.Point, int) {
	if curr == nil || prev == nil {
		return image.Pt(0, 0), 0
	}
	b := curr.Bounds()
	x0, y0 := 0, 0
	x1, y1 := b.Dx(), b.Dy()
	bestScore := math.Inf(1)
	bestSamples := 0
	bestVec := image.Pt(0, 0)
	for vy := -searchRadius; vy <= searchRadius; vy++ {
		for vx := -searchRadius; vx <= searchRadius; vx++ {
			score, samples := compareMotionWindow(curr, prev, infillMask, x0, y0, x1, y1, vx, vy)
			if samples < 80 {
				continue
			}
			if score < bestScore {
				bestScore = score
				bestSamples = samples
				bestVec = image.Pt(vx, vy)
			}
		}
	}
	return bestVec, bestSamples
}

func estimateMotionVectors(curr, prev *image.RGBA, infillMask []bool, bounds image.Rectangle) map[image.Point]image.Point {
	if curr == nil || prev == nil || infillMask == nil {
		return nil
	}
	const (
		searchRadius = 8
		contextPad   = 12
		minSamples   = 24
	)

	globalVec, globalSamples := estimateGlobalMotionVector(curr, prev, infillMask, searchRadius)
	vectors := make(map[image.Point]image.Point)

	for by := 0; by < bounds.Dy(); by += motionBlockSize {
		for bx := 0; bx < bounds.Dx(); bx += motionBlockSize {
			if !blockHasInfillPixels(infillMask, bx, by, motionBlockSize, motionBlockSize, bounds) {
				continue
			}

			winX0 := max(0, bx-contextPad)
			winY0 := max(0, by-contextPad)
			winX1 := minInt(bounds.Dx(), bx+motionBlockSize+contextPad)
			winY1 := minInt(bounds.Dy(), by+motionBlockSize+contextPad)

			bestScore := math.Inf(1)
			bestSamples := 0
			bestVec := image.Pt(0, 0)
			for vy := -searchRadius; vy <= searchRadius; vy++ {
				for vx := -searchRadius; vx <= searchRadius; vx++ {
					score, samples := compareMotionWindow(curr, prev, infillMask, winX0, winY0, winX1, winY1, vx, vy)
					if samples < minSamples {
						continue
					}
					if score < bestScore {
						bestScore = score
						bestSamples = samples
						bestVec = image.Pt(vx, vy)
					}
				}
			}

			if bestSamples >= minSamples {
				vectors[image.Pt((bx/motionBlockSize)*motionBlockSize, (by/motionBlockSize)*motionBlockSize)] = bestVec
				continue
			}
			if globalSamples >= minSamples {
				vectors[image.Pt((bx/motionBlockSize)*motionBlockSize, (by/motionBlockSize)*motionBlockSize)] = globalVec
			}
		}
	}

	if len(vectors) == 0 {
		return nil
	}
	return vectors
}

func processGIF(g *gif.GIF, ref image.Image, mask image.Image, cityMask image.Image, latestOnly bool) []frameInfo {
	started := time.Now()
	bounds := image.Rect(0, 0, g.Config.Width, g.Config.Height)
	batchTime := time.Now()
	batchID := batchTime.Format("20060102_150405")
	log.Printf("[process:%s] start: gif_frames=%d latestOnly=%t bounds=%dx%d", batchID, len(g.Image), latestOnly, bounds.Dx(), bounds.Dy())

	var cleanHistory *image.RGBA
	cityInfillMask := buildCityInfillMask(cityMask, bounds)
	if cityInfillMask != nil {
		log.Printf("[process:%s] city infill mask prepared", batchID)
	}

	// 1) Compose all GIF frames first (respecting disposal rules).
	stageComposeStart := time.Now()
	composed := make([]*image.Paletted, len(g.Image))
	var prev *image.Paletted
	for i, src := range g.Image {
		frame := compositeFrame(bounds, prev, src, g.Disposal, i)
		composed[i] = frame
		prev = frame
	}
	log.Printf("[process:%s] stage compose done: frames=%d elapsed=%s", batchID, len(composed), time.Since(stageComposeStart).Round(time.Millisecond))

	// 2) Build base precipitation layer without infill (used for motion estimation).
	stageBaseStart := time.Now()
	baseFrames := make([]*image.RGBA, len(composed))
	lightningMasks := make([][]bool, len(composed))
	for i := range composed {
		var prev image.Image
		var next image.Image
		if i > 0 {
			prev = composed[i-1]
		}
		if i+1 < len(composed) {
			next = composed[i+1]
		}
		if !keepLightningPluses {
			lightningMasks[i] = detectLightningGlyphMask(composed[i], prev, next, bounds)
		}
		baseFrames[i] = subtractReference(composed[i], ref, mask, nil, bounds, nil, nil, nil, nil, nil, nil, nil, nil)
	}
	log.Printf("[process:%s] stage base done: frames=%d elapsed=%s", batchID, len(baseFrames), time.Since(stageBaseStart).Round(time.Millisecond))

	// 3) Apply infill (city labels + lightning glyphs) from temporal neighbours.
	stageInfillStart := time.Now()
	outFrames := make([]*image.RGBA, len(composed))
	for i := range composed {
		infillMask := mergeInfillMasks(cityInfillMask, lightningMasks[i])
		if infillMask == nil {
			outFrames[i] = baseFrames[i]
			continue
		}

		var prevFrame *image.RGBA
		var nextFrame *image.RGBA
		var prevArtifactMask []bool
		var nextArtifactMask []bool
		var motionPrev map[image.Point]image.Point
		var motionNext map[image.Point]image.Point
		if i > 0 {
			prevFrame = baseFrames[i-1]
			prevArtifactMask = lightningMasks[i-1]
			motionPrev = estimateMotionVectors(baseFrames[i], prevFrame, infillMask, bounds)
		}
		if i+1 < len(baseFrames) {
			nextFrame = baseFrames[i+1]
			nextArtifactMask = lightningMasks[i+1]
			motionNext = estimateMotionVectors(baseFrames[i], nextFrame, infillMask, bounds)
		}

		out := subtractReference(
			composed[i], ref, mask, nil, bounds,
			prevFrame, nextFrame, cleanHistory,
			infillMask, prevArtifactMask, nextArtifactMask,
			motionPrev, motionNext,
		)
		fillLightningMaskHoles(out, lightningMasks[i], composed[i])
		infillCityMaskNowcast(
			out,
			cityInfillMask,
			prevFrame,
			nextFrame,
			cleanHistory,
			prevArtifactMask,
			nextArtifactMask,
			motionPrev,
			motionNext,
		)
		cleanupCityMaskResiduals(out, cityInfillMask, prevFrame, nextFrame)
		mergeIntoCleanHistory(out, bounds, &cleanHistory)
		outFrames[i] = out
	}
	log.Printf("[process:%s] stage infill done: frames=%d elapsed=%s", batchID, len(outFrames), time.Since(stageInfillStart).Round(time.Millisecond))

	// Global temporal stabilization reduces frame-to-frame flicker outside infill zones.
	stageStabilizeStart := time.Now()
	temporalStabilizeFrames(outFrames, 1)
	// Temporal stabilization may re-introduce short tails under city labels.
	// Run one more cleanup pass against stabilized neighbors.
	for i := range outFrames {
		var prevStable *image.RGBA
		var nextStable *image.RGBA
		if i > 0 {
			prevStable = outFrames[i-1]
		}
		if i+1 < len(outFrames) {
			nextStable = outFrames[i+1]
		}
		cleanupCityMaskResiduals(outFrames[i], cityInfillMask, prevStable, nextStable)
	}
	cleanupConvectiveSpeckles(outFrames)
	log.Printf("[process:%s] stage stabilize+cleanup done: elapsed=%s", batchID, time.Since(stageStabilizeStart).Round(time.Millisecond))

	metrics := computeFrameBatchMetrics(outFrames)
	log.Printf(
		"[process:%s] frame metrics: coverage=%.4f flicker=%.4f edgeRough=%.4f meanAlpha=%.1f frames=%d",
		batchID,
		metrics.CoverageMean, metrics.FlickerRate, metrics.EdgeRoughness, metrics.MeanAlpha, metrics.FrameCount,
	)
	if metrics.FlickerRate > 0.085 {
		log.Printf("[process:%s] frame quality alert: high flicker rate %.4f", batchID, metrics.FlickerRate)
	}

	saveWarpStart := time.Now()
	savedCount := 0
	warpedCount := 0
	skippedUnchanged := 0
	for i, out := range outFrames {
		// Fix: Ensure we use the correct batch ID for all frames or per-frame?
		// The user code had batchID generated once.
		filename := framePrefix + batchID + "_" + padInt(i, 3) + ".png"
		path := filepath.Join(framesDir, filename)
		warpedPath := filepath.Join(warpedDir, filename)
		shouldSave := !latestOnly || i == len(g.Image)-1
		if !shouldSave {
			continue
		}

		digest := hashRGBA(out)
		if latestOnly {
			state.mu.RLock()
			unchanged := state.hasLastFrameHash && state.lastFrameDigest == digest
			state.mu.RUnlock()
			if unchanged {
				skippedUnchanged++
				log.Printf("[process:%s] frame %d unchanged -> skip save", batchID, i)
				continue
			}
		}

		if err := savePNG(path, out); err != nil {
			log.Printf("[process:%s] save frame %d failed: %v", batchID, i, err)
			continue
		}
		savedCount++
		log.Printf("[process:%s] frame %d saved: %s", batchID, i, path)
		if thinSplineEnabled {
			if err := warpFrameThinSpline(path, warpedPath); err != nil {
				log.Printf("[process:%s] warp frame %d failed: %v", batchID, i, err)
			} else {
				warpedCount++
				log.Printf("[process:%s] frame %d warped: %s", batchID, i, warpedPath)
			}
		}

		state.mu.Lock()
		state.lastFrameDigest = digest
		state.hasLastFrameHash = true
		state.mu.Unlock()
	}
	log.Printf(
		"[process:%s] stage save+warp done: saved=%d warped=%d skipped_unchanged=%d elapsed=%s",
		batchID,
		savedCount,
		warpedCount,
		skippedUnchanged,
		time.Since(saveWarpStart).Round(time.Millisecond),
	)

	trimmed := enforceMeteoFrameWindow(meteoFrameWindow)
	if trimmed > 0 {
		log.Printf("[process:%s] trimmed oldest meteo frames: %d", batchID, trimmed)
	}
	cleanupOldFrames()
	scanned := scanFrames()
	log.Printf("[process:%s] done: indexed_frames=%d total_elapsed=%s", batchID, len(scanned), time.Since(started).Round(time.Millisecond))
	return scanned
}

func cleanupOldFrames() {
	cutoff := time.Now().Add(-3 * time.Hour)
	cleanupDirByCutoff(framesDir, cutoff, true)
	if thinSplineEnabled {
		cleanupDirByCutoff(warpedDir, cutoff, true)
	}
}

func enforceMeteoFrameWindow(limit int) int {
	if limit <= 0 {
		return 0
	}
	trimRaw := trimOldestFrameFiles(framesDir, limit)
	trimWarp := 0
	if thinSplineEnabled {
		trimWarp = trimOldestFrameFiles(warpedDir, limit)
	}
	return trimRaw + trimWarp
}

func trimOldestFrameFiles(dir string, limit int) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	type item struct {
		path string
		mod  time.Time
	}
	list := make([]item, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(strings.ToLower(e.Name()), ".png") {
			continue
		}
		if e.Name() == warpedMaskFileName {
			continue
		}
		path := filepath.Join(dir, e.Name())
		info, err := os.Stat(path)
		if err != nil {
			continue
		}
		list = append(list, item{path: path, mod: info.ModTime()})
	}
	if len(list) <= limit {
		return 0
	}
	sort.Slice(list, func(i, j int) bool { return list[i].mod.Before(list[j].mod) })
	removeCount := len(list) - limit
	removed := 0
	for i := 0; i < removeCount; i++ {
		path := list[i].path
		if err := os.Remove(path); err != nil {
			continue
		}
		_ = os.Remove(strings.TrimSuffix(path, ".png") + ".json")
		removed++
	}
	return removed
}

func cleanupDirByCutoff(dir string, cutoff time.Time, removeJSON bool) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		path := filepath.Join(dir, e.Name())
		info, err := os.Stat(path)
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			_ = os.Remove(path)
			if removeJSON && strings.HasSuffix(e.Name(), ".png") {
				_ = os.Remove(strings.TrimSuffix(path, ".png") + ".json")
			}
		}
	}
}

func scanFrames() []frameInfo {
	sourceDir := framesDir
	urlPrefix := "/frames/"
	if thinSplineEnabled {
		sourceDir = warpedDir
		urlPrefix = "/frames_warped/"
	}

	entries, err := os.ReadDir(sourceDir)
	if err != nil {
		return nil
	}

	type frameFile struct {
		name string
		mod  time.Time
	}
	var list []frameFile
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".png") {
			continue
		}
		if thinSplineEnabled && e.Name() == warpedMaskFileName {
			continue
		}
		path := filepath.Join(sourceDir, e.Name())
		info, err := os.Stat(path)
		if err != nil {
			continue
		}
		list = append(list, frameFile{e.Name(), info.ModTime()})
	}

	// Если варп-кадры по какой-то причине не получились, мягко откатываемся на raw frames.
	if thinSplineEnabled && len(list) == 0 {
		entries, err = os.ReadDir(framesDir)
		if err != nil {
			return nil
		}
		sourceDir = framesDir
		urlPrefix = "/frames/"
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".png") {
				continue
			}
			if e.Name() == warpedMaskFileName {
				continue
			}
			path := filepath.Join(sourceDir, e.Name())
			info, err := os.Stat(path)
			if err != nil {
				continue
			}
			list = append(list, frameFile{e.Name(), info.ModTime()})
		}
	}

	sort.Slice(list, func(i, j int) bool { return list[i].mod.Before(list[j].mod) })

	cutoff := time.Now().Add(-3 * time.Hour)
	var listFiltered []frameFile
	for _, e := range list {
		if e.mod.Before(cutoff) {
			continue
		}
		listFiltered = append(listFiltered, e)
	}

	now := time.Now().UTC()
	delay := time.Duration(gifDelayMin) * time.Minute
	interval := time.Duration(frameIntervalMin) * time.Minute
	cutoffTime := now.Add(-delay)
	// Округляем вниз до границы 10 минут (:00, :10, :20, ...)
	lastFrameMinute := (cutoffTime.Minute() / frameIntervalMin) * frameIntervalMin
	lastFrameTime := time.Date(cutoffTime.Year(), cutoffTime.Month(), cutoffTime.Day(),
		cutoffTime.Hour(), lastFrameMinute, 0, 0, time.UTC)

	frames := make([]frameInfo, 0, len(listFiltered))
	for i, e := range listFiltered {
		n := len(listFiltered)
		// Последний кадр (i==n-1) = lastFrameTime, предпоследний = lastFrameTime-10min, ...
		frameTime := lastFrameTime.Add(-interval * time.Duration(n-1-i))
		frames = append(frames, frameInfo{
			URL:   urlPrefix + e.name,
			Time:  frameTime.Format(time.RFC3339),
			Index: i,
		})
	}
	return frames
}

func compositeFrame(bounds image.Rectangle, prev, curr *image.Paletted, disposal []byte, idx int) *image.Paletted {
	dst := image.NewPaletted(bounds, curr.Palette)

	if prev != nil {
		draw.Draw(dst, bounds, prev, bounds.Min, draw.Src)
	}

	switch {
	case idx < len(disposal):
		switch disposal[idx] {
		case gif.DisposalPrevious:
			// keep prev, draw curr over
			if prev != nil {
				draw.Draw(dst, bounds, prev, bounds.Min, draw.Src)
			}
		case gif.DisposalBackground:
			// clear to background (transparent or bg color)
			draw.Draw(dst, bounds, &image.Uniform{color.Transparent}, bounds.Min, draw.Src)
		}
	}

	currBounds := curr.Bounds()
	draw.Draw(dst, currBounds, curr, currBounds.Min, draw.Over)

	return dst
}

// mergeIntoCleanHistory merges high-saturation pixels from out into cleanHistory.
func mergeIntoCleanHistory(out *image.RGBA, bounds image.Rectangle, cleanHistory **image.RGBA) {
	dx, dy := bounds.Dx(), bounds.Dy()
	if *cleanHistory == nil {
		*cleanHistory = image.NewRGBA(bounds)
	}
	for y := 0; y < dy; y++ {
		for x := 0; x < dx; x++ {
			pc := out.RGBAAt(x, y)
			if pc.A == 0 {
				continue
			}
			maxC := pc.R
			if pc.G > maxC {
				maxC = pc.G
			}
			if pc.B > maxC {
				maxC = pc.B
			}
			minC := pc.R
			if pc.G < minC {
				minC = pc.G
			}
			if pc.B < minC {
				minC = pc.B
			}
			if (maxC - minC) < cleanHistorySatMerge {
				continue
			}
			hc := (*cleanHistory).RGBAAt(x, y)
			if hc.A == 0 {
				(*cleanHistory).SetRGBA(x, y, color.RGBA{pc.R, pc.G, pc.B, 255})
				continue
			}
			hMax := hc.R
			if hc.G > hMax {
				hMax = hc.G
			}
			if hc.B > hMax {
				hMax = hc.B
			}
			hMin := hc.R
			if hc.G < hMin {
				hMin = hc.G
			}
			if hc.B < hMin {
				hMin = hc.B
			}
			if (maxC - minC) > (hMax - hMin) {
				(*cleanHistory).SetRGBA(x, y, color.RGBA{pc.R, pc.G, pc.B, 255})
			}
		}
	}
}

// isCityPixel returns true if (x,y) in cityMask is in city zone (dark).
func isCityPixel(cityMask image.Image, x, y int) bool {
	if cityMask == nil {
		return false
	}
	b := cityMask.Bounds()
	if x < b.Min.X || x >= b.Max.X || y < b.Min.Y || y >= b.Max.Y {
		return false
	}
	cr, cg, cb, _ := cityMask.At(x, y).RGBA()
	avg := (cr + cg + cb) / 3
	return avg < cityMaskDark
}

func isUsefulPrecipPixel(c color.RGBA, satThreshold uint8) bool {
	if c.A == 0 {
		return false
	}
	maxC := c.R
	if c.G > maxC {
		maxC = c.G
	}
	if c.B > maxC {
		maxC = c.B
	}
	minC := c.R
	if c.G < minC {
		minC = c.G
	}
	if c.B < minC {
		minC = c.B
	}
	return (maxC - minC) > satThreshold
}

func subtractReference(
	src *image.Paletted,
	ref image.Image,
	mask image.Image,
	cityMask image.Image,
	bounds image.Rectangle,
	prevFrame *image.RGBA,
	nextFrame *image.RGBA,
	cleanHistory *image.RGBA,
	infillMask []bool,
	prevArtifactMask []bool,
	nextArtifactMask []bool,
	motionVectorsPrev map[image.Point]image.Point,
	motionVectorsNext map[image.Point]image.Point,
) *image.RGBA {
	dst := image.NewRGBA(bounds)
	dx, dy := bounds.Dx(), bounds.Dy()

	// Full clear: ensure dst is fully transparent canvas
	for y := 0; y < dy; y++ {
		for x := 0; x < dx; x++ {
			dst.SetRGBA(x, y, color.RGBA{0, 0, 0, 0})
		}
	}

	const refTolerance = 4500 // aggressive: tolerate more JPEG/compression diff
	const satThreshold = 75   // aggressive: only high-saturation pixels pass
	const maskThreshold = 5000
	const cityInfillSatThreshold = 20 // for city zone: accept weaker precip when restoring

	for y := 0; y < dy; y++ {
		for x := 0; x < dx; x++ {
			inInfillZone := false
			if infillMask != nil {
				inInfillZone = infillMask[y*dx+x]
			} else if cityMask != nil {
				// Fallback to static city mask if explicit infill mask wasn't provided.
				inInfillZone = isCityPixel(cityMask, x, y)
			}

			if inInfillZone {
				// --- City zone: motion-compensated infill from prevFrame ---
				if debugCityRed {
					dst.SetRGBA(x, y, color.RGBA{255, 0, 0, 255})
					continue
				}

				tryRestore := func(source *image.RGBA, vectors map[image.Point]image.Point, sourceArtifactMask []bool) (color.RGBA, int, bool) {
					if source == nil {
						return color.RGBA{}, 0, false
					}
					pastX, pastY := x, y
					blockPt := image.Pt((x/motionBlockSize)*motionBlockSize, (y/motionBlockSize)*motionBlockSize)
					if vectors != nil {
						if v, ok := vectors[blockPt]; ok {
							pastX = x - v.X
							pastY = y - v.Y
						}
					}
					if pastX < 0 || pastX >= dx || pastY < 0 || pastY >= dy {
						return color.RGBA{}, 0, false
					}
					// Do not restore from a donor pixel that is itself a lightning glyph.
					if maskHasPixel(sourceArtifactMask, dx, dy, pastX, pastY) {
						return color.RGBA{}, 0, false
					}
					pc := source.RGBAAt(pastX, pastY)
					if !isUsefulPrecipPixel(pc, cityInfillSatThreshold) {
						return color.RGBA{}, 0, false
					}

					// Local support: avoid spreading isolated noisy pixel across whole label.
					support := 0
					for oy := -1; oy <= 1; oy++ {
						for ox := -1; ox <= 1; ox++ {
							nx := pastX + ox
							ny := pastY + oy
							if nx < 0 || nx >= dx || ny < 0 || ny >= dy {
								continue
							}
							if isUsefulPrecipPixel(source.RGBAAt(nx, ny), cityInfillSatThreshold) {
								support++
							}
						}
					}
					if support < 3 {
						return color.RGBA{}, 0, false
					}
					return color.RGBA{pc.R, pc.G, pc.B, 255}, support, true
				}

				prevC, prevSupport, prevOK := tryRestore(prevFrame, motionVectorsPrev, prevArtifactMask)
				nextC, nextSupport, nextOK := tryRestore(nextFrame, motionVectorsNext, nextArtifactMask)
				histOK := false
				histC := color.RGBA{}
				if cleanHistory != nil {
					hc := cleanHistory.RGBAAt(x, y)
					if isUsefulPrecipPixel(hc, cityInfillSatThreshold) {
						histC = color.RGBA{R: hc.R, G: hc.G, B: hc.B, A: 255}
						histOK = true
					}
				}

				switch {
				case prevOK && (!nextOK || prevSupport >= nextSupport):
					dst.SetRGBA(x, y, prevC)
				case nextOK:
					dst.SetRGBA(x, y, nextC)
				case histOK:
					dst.SetRGBA(x, y, histC)
				default:
					dst.SetRGBA(x, y, color.RGBA{0, 0, 0, 0})
				}
				continue
			}

			// --- Non-city zone: standard filters (mask + reference + saturation) ---
			c := src.At(x, y)
			r32, g32, b32, a32 := c.RGBA()

			if a32 == 0 {
				dst.SetRGBA(x, y, color.RGBA{0, 0, 0, 0})
				continue
			}
			shouldDiscard := false

			// 1. Image Mask (Strict Black Check)
			if !shouldDiscard && mask != nil {
				maskBounds := mask.Bounds()
				if x < maskBounds.Dx() && y < maskBounds.Dy() {
					mr, mg, mb, _ := mask.At(x, y).RGBA()
					// If NOT black (any channel > threshold), discard
					if mr > maskThreshold || mg > maskThreshold || mb > maskThreshold {
						shouldDiscard = true
					}
				}
			}

			// 2. Reference Comparison
			if !shouldDiscard && ref != nil {
				refBounds := ref.Bounds()
				if x < refBounds.Dx() && y < refBounds.Dy() {
					rr, rg, rb, _ := ref.At(x, y).RGBA()
					diff := func(a, b uint32) uint32 {
						if a > b {
							return a - b
						}
						return b - a
					}
					if diff(r32, rr) < refTolerance && diff(g32, rg) < refTolerance && diff(b32, rb) < refTolerance {
						shouldDiscard = true
					}
				}
			}

			// 3. Saturation Filter
			if !shouldDiscard {
				r, g, b := uint8(r32>>8), uint8(g32>>8), uint8(b32>>8)
				// Удаляем "плюс"-иконки только при отсутствии осадковой поддержки вокруг.
				// Это защищает реальные град/шквал/грозовые пиксели от ложного вырезания.
				if isLightningGlyphColor(color.RGBA{R: r, G: g, B: b, A: 255}) &&
					!hasCarrierPrecipSupport(src, bounds, x, y) {
					shouldDiscard = true
				}
				maxC := r
				if g > maxC {
					maxC = g
				}
				if b > maxC {
					maxC = b
				}
				minC := r
				if g < minC {
					minC = g
				}
				if b < minC {
					minC = b
				}
				if (maxC - minC) < satThreshold {
					shouldDiscard = true
				}
			}

			// --- DECISION ---
			// No Temporal Infill in non-city zones: filtered pixels become transparent
			if !shouldDiscard {
				r, g, b := uint8(r32>>8), uint8(g32>>8), uint8(b32>>8)
				dst.SetRGBA(x, y, color.RGBA{r, g, b, 255})
			} else {
				dst.SetRGBA(x, y, color.RGBA{0, 0, 0, 0})
			}
		}
	}

	// Финальный штрих: восстанавливаем мелкие пропуски внутри облаков
	fillTinyHolesFromRaw(dst, src, 3)
	fillEnclosedTransparentHoles(dst, src, 140)
	softSnapToLegend(dst)

	return dst
}

func fillSmallHoles(dst *image.RGBA) {
	bounds := dst.Bounds()
	dx, dy := bounds.Dx(), bounds.Dy()

	// Создаем копию, чтобы изменения не влияли на проверку соседей в рамках одного прохода
	tmp := image.NewRGBA(bounds)
	copy(tmp.Pix, dst.Pix)

	for y := 1; y < dy-1; y++ {
		for x := 1; x < dx-1; x++ {
			// Если в обработанном кадре здесь пусто
			if dst.RGBAAt(x, y).A == 0 {
				neighborCount := 0

				// Считаем закрашенных соседей в квадрате 3x3
				for oy := -1; oy <= 1; oy++ {
					for ox := -1; ox <= 1; ox++ {
						if ox == 0 && oy == 0 {
							continue
						}
						if dst.RGBAAt(x+ox, y+oy).A > 0 {
							neighborCount++
						}
					}
				}

				// Порог: 6 из 8 соседей должны быть осадками
				if neighborCount >= 6 {
					// Заполнение только цветами из легенды:
					// соседей квантуем в ближайший класс, затем берем мажоритарный класс.
					type key [3]uint8
					votes := make(map[key]int)
					for oy := -1; oy <= 1; oy++ {
						for ox := -1; ox <= 1; ox++ {
							if ox == 0 && oy == 0 {
								continue
							}
							c := dst.RGBAAt(x+ox, y+oy)
							if c.A == 0 {
								continue
							}
							nc := nearestPhenomenaColor(c)
							k := key{nc.R, nc.G, nc.B}
							votes[k]++
						}
					}
					if len(votes) > 0 {
						var best key
						bestN := -1
						for k, n := range votes {
							if n > bestN {
								bestN = n
								best = k
							}
						}
						tmp.SetRGBA(x, y, color.RGBA{R: best[0], G: best[1], B: best[2], A: 255})
					}
				}
			}
		}
	}
	// Переносим результат обратно в основной буфер
	copy(dst.Pix, tmp.Pix)
}

// fillTinyHolesFromRaw restores transparent holes up to maxArea pixels
// from the original unprocessed frame (raw GIF composition).
func fillTinyHolesFromRaw(dst *image.RGBA, raw image.Image, maxArea int) {
	if dst == nil || raw == nil || maxArea <= 0 {
		return
	}
	b := dst.Bounds()
	w, h := b.Dx(), b.Dy()
	if w < 3 || h < 3 {
		return
	}

	visited := make([]bool, w*h)
	neighbors4 := [][2]int{{1, 0}, {-1, 0}, {0, 1}, {0, -1}}

	for y := 1; y < h-1; y++ {
		for x := 1; x < w-1; x++ {
			start := y*w + x
			if visited[start] || dst.RGBAAt(x, y).A > 0 {
				continue
			}

			queue := []int{start}
			visited[start] = true
			region := make([]int, 0, 8)
			region = append(region, start)
			touchesEdge := false

			for q := 0; q < len(queue); q++ {
				i := queue[q]
				cx := i % w
				cy := i / w
				if cx == 0 || cy == 0 || cx == w-1 || cy == h-1 {
					touchesEdge = true
				}
				for _, d := range neighbors4 {
					nx := cx + d[0]
					ny := cy + d[1]
					if nx < 0 || nx >= w || ny < 0 || ny >= h {
						continue
					}
					ni := ny*w + nx
					if visited[ni] || dst.RGBAAt(nx, ny).A > 0 {
						continue
					}
					visited[ni] = true
					queue = append(queue, ni)
					region = append(region, ni)
					if len(region) > maxArea {
						break
					}
				}
				if len(region) > maxArea {
					break
				}
			}

			if touchesEdge || len(region) == 0 || len(region) > maxArea {
				continue
			}

			// Confirm the hole is enclosed by precipitation in processed frame.
			enclosed := true
			for _, i := range region {
				cx := i % w
				cy := i / w
				localSupport := 0
				for _, d := range neighbors4 {
					nx := cx + d[0]
					ny := cy + d[1]
					if nx < 0 || nx >= w || ny < 0 || ny >= h {
						continue
					}
					if dst.RGBAAt(nx, ny).A > 0 {
						localSupport++
					}
				}
				if localSupport < 2 {
					enclosed = false
					break
				}
			}
			if !enclosed {
				continue
			}

			// Restore directly from original frame at the same pixels.
			for _, i := range region {
				cx := i % w
				cy := i / w
				rr, rg, rb, ra := raw.At(cx+b.Min.X, cy+b.Min.Y).RGBA()
				if ra == 0 {
					continue
				}
				rawColor := color.RGBA{
					R: uint8(rr >> 8),
					G: uint8(rg >> 8),
					B: uint8(rb >> 8),
					A: 255,
				}
				if !isUsefulPrecipPixel(rawColor, 12) {
					continue
				}
				dst.SetRGBA(cx, cy, rawColor)
			}
		}
	}
}

// fillEnclosedTransparentHoles closes small fully enclosed transparent islands
// by restoring pixels from the original raw frame at the same coordinates.
func fillEnclosedTransparentHoles(dst *image.RGBA, raw image.Image, maxArea int) {
	if dst == nil || raw == nil || maxArea <= 0 {
		return
	}
	bounds := dst.Bounds()
	w, h := bounds.Dx(), bounds.Dy()
	if w < 3 || h < 3 {
		return
	}

	visited := make([]bool, w*h)
	neighbors4 := [][2]int{{1, 0}, {-1, 0}, {0, 1}, {0, -1}}
	neighbors8 := [][2]int{
		{1, 0}, {-1, 0}, {0, 1}, {0, -1},
		{1, 1}, {1, -1}, {-1, 1}, {-1, -1},
	}
	type key [3]uint8

	for y := 1; y < h-1; y++ {
		for x := 1; x < w-1; x++ {
			start := y*w + x
			if visited[start] || dst.RGBAAt(x, y).A > 0 {
				continue
			}

			queue := []int{start}
			visited[start] = true
			region := make([]int, 0, 64)
			region = append(region, start)
			touchesEdge := false

			for q := 0; q < len(queue); q++ {
				i := queue[q]
				cx := i % w
				cy := i / w
				if cx == 0 || cy == 0 || cx == w-1 || cy == h-1 {
					touchesEdge = true
				}
				for _, d := range neighbors4 {
					nx := cx + d[0]
					ny := cy + d[1]
					if nx < 0 || nx >= w || ny < 0 || ny >= h {
						continue
					}
					ni := ny*w + nx
					if visited[ni] || dst.RGBAAt(nx, ny).A > 0 {
						continue
					}
					visited[ni] = true
					queue = append(queue, ni)
					region = append(region, ni)
					if len(region) > maxArea {
						break
					}
				}
				if len(region) > maxArea {
					break
				}
			}

			if touchesEdge || len(region) == 0 || len(region) > maxArea {
				continue
			}

			support := 0
			for _, i := range region {
				cx := i % w
				cy := i / w
				for _, d := range neighbors8 {
					nx := cx + d[0]
					ny := cy + d[1]
					if nx < 0 || nx >= w || ny < 0 || ny >= h {
						continue
					}
					c := dst.RGBAAt(nx, ny)
					if isUsefulPrecipPixel(c, 16) {
						support++
					}
				}
			}
			if support < len(region)*3 {
				continue
			}

			for _, i := range region {
				cx := i % w
				cy := i / w
				rr, rg, rb, ra := raw.At(cx+bounds.Min.X, cy+bounds.Min.Y).RGBA()
				if ra == 0 {
					continue
				}
				rawColor := color.RGBA{
					R: uint8(rr >> 8),
					G: uint8(rg >> 8),
					B: uint8(rb >> 8),
					A: 255,
				}
				if !isUsefulPrecipPixel(rawColor, 12) {
					continue
				}
				dst.SetRGBA(cx, cy, rawColor)
			}
		}
	}
}

// fillLightningMaskHoles restores transparent pixels inside detected lightning-glyph
// mask from the original raw frame.
func fillLightningMaskHoles(dst *image.RGBA, lightningMask []bool, raw image.Image) {
	if dst == nil || lightningMask == nil || raw == nil {
		return
	}
	b := dst.Bounds()
	w, h := b.Dx(), b.Dy()
	if len(lightningMask) < w*h {
		return
	}

	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			idx := y*w + x
			if !lightningMask[idx] || dst.RGBAAt(x, y).A > 0 {
				continue
			}
			rr, rg, rb, ra := raw.At(x+b.Min.X, y+b.Min.Y).RGBA()
			if ra == 0 {
				continue
			}
			rawColor := color.RGBA{
				R: uint8(rr >> 8),
				G: uint8(rg >> 8),
				B: uint8(rb >> 8),
				A: 255,
			}
			if !isUsefulPrecipPixel(rawColor, 12) {
				continue
			}
			dst.SetRGBA(x, y, rawColor)
		}
	}
}

// infillCityMaskNowcast restores precipitation under city labels (city mask),
// where source radar frame has no true signal due to drawn map text.
// Priority: motion-compensated prev/next -> clean history -> local neighborhood vote.
func infillCityMaskNowcast(
	dst *image.RGBA,
	cityMask []bool,
	prevFrame *image.RGBA,
	nextFrame *image.RGBA,
	cleanHistory *image.RGBA,
	prevArtifactMask []bool,
	nextArtifactMask []bool,
	motionVectorsPrev map[image.Point]image.Point,
	motionVectorsNext map[image.Point]image.Point,
) {
	if dst == nil || cityMask == nil {
		return
	}
	b := dst.Bounds()
	w, h := b.Dx(), b.Dy()
	if len(cityMask) < w*h {
		return
	}

	type key [3]uint8
	for pass := 0; pass < 3; pass++ {
		changed := 0
		tmp := image.NewRGBA(b)
		copy(tmp.Pix, dst.Pix)
		for y := 0; y < h; y++ {
			for x := 0; x < w; x++ {
				idx := y*w + x
				if !cityMask[idx] || dst.RGBAAt(x, y).A > 0 {
					continue
				}

				bestScore := -1
				bestColor := color.RGBA{}

				if c, s, ok := sampleMotionDonor(prevFrame, motionVectorsPrev, prevArtifactMask, x, y, w, h, 10); ok {
					score := 30 + s*3
					if score > bestScore {
						bestScore = score
						bestColor = c
					}
				}
				if c, s, ok := sampleMotionDonor(nextFrame, motionVectorsNext, nextArtifactMask, x, y, w, h, 10); ok {
					score := 28 + s*3
					if score > bestScore {
						bestScore = score
						bestColor = c
					}
				}

				if cleanHistory != nil {
					hc := cleanHistory.RGBAAt(x, y)
					if isUsefulPrecipPixel(hc, 10) {
						if legendColor, ok := mapToLegendColorInfill(color.RGBA{R: hc.R, G: hc.G, B: hc.B, A: 255}); ok {
							score := 20
							if score > bestScore {
								bestScore = score
								bestColor = legendColor
							}
						}
					}
				}

				// Fallback: infer from nearest valid neighborhood around city label.
				votes := make(map[key]int)
				support := 0
				for oy := -4; oy <= 4; oy++ {
					for ox := -4; ox <= 4; ox++ {
						if ox == 0 && oy == 0 {
							continue
						}
						nx := x + ox
						ny := y + oy
						if nx < 0 || nx >= w || ny < 0 || ny >= h {
							continue
						}
						nidx := ny*w + nx
						// Prefer donors outside the city text mask to avoid echoing holes.
						if cityMask[nidx] {
							continue
						}
						c := dst.RGBAAt(nx, ny)
						if !isUsefulPrecipPixel(c, 16) {
							continue
						}
						nc := nearestPhenomenaColor(c)
						if isTornadoClass(nc) {
							continue
						}
						weight := 1
						manhattan := absInt(ox) + absInt(oy)
						if manhattan <= 2 {
							weight = 3
						} else if manhattan <= 4 {
							weight = 2
						}
						votes[key{nc.R, nc.G, nc.B}] += weight
						support += weight
					}
				}
				if support >= 6 && len(votes) > 0 {
					var vk key
					bestVotes := -1
					for k, n := range votes {
						if n > bestVotes {
							bestVotes = n
							vk = k
						}
					}
					score := 12 + bestVotes
					if score > bestScore {
						bestScore = score
						bestColor = color.RGBA{R: vk[0], G: vk[1], B: vk[2], A: 255}
					}
				}

				if bestScore >= 0 {
					tmp.SetRGBA(x, y, bestColor)
					changed++
				}
			}
		}
		copy(dst.Pix, tmp.Pix)
		if changed == 0 {
			break
		}
	}
}

// cleanupCityMaskResiduals removes stale leftovers inside city labels after the
// precipitation area moved away. Pixel is kept only if supported by nearby
// non-city precipitation or by temporal donors.
func cleanupCityMaskResiduals(
	dst *image.RGBA,
	cityMask []bool,
	prevFrame *image.RGBA,
	nextFrame *image.RGBA,
) {
	if dst == nil || cityMask == nil {
		return
	}
	b := dst.Bounds()
	w, h := b.Dx(), b.Dy()
	if len(cityMask) < w*h {
		return
	}

	for pass := 0; pass < 2; pass++ {
		tmp := image.NewRGBA(b)
		copy(tmp.Pix, dst.Pix)
		changed := 0
		for y := 0; y < h; y++ {
			for x := 0; x < w; x++ {
				idx := y*w + x
				c := dst.RGBAAt(x, y)
				if !cityMask[idx] || c.A == 0 {
					continue
				}

				outsideSupport := 0
				for oy := -3; oy <= 3; oy++ {
					for ox := -3; ox <= 3; ox++ {
						if ox == 0 && oy == 0 {
							continue
						}
						nx := x + ox
						ny := y + oy
						if nx < 0 || nx >= w || ny < 0 || ny >= h {
							continue
						}
						nidx := ny*w + nx
						if cityMask[nidx] {
							continue
						}
						nc := dst.RGBAAt(nx, ny)
						if isUsefulPrecipPixel(nc, 10) {
							outsideSupport++
						}
					}
				}

				temporalSupport := 0
				if prevFrame != nil && isUsefulPrecipPixel(prevFrame.RGBAAt(x, y), 10) {
					temporalSupport++
				}
				if nextFrame != nil && isUsefulPrecipPixel(nextFrame.RGBAAt(x, y), 10) {
					temporalSupport++
				}

				// Do not let city-mask pixels self-sustain each other:
				// keep only when there is support outside label area or temporal evidence.
				if outsideSupport == 0 && temporalSupport == 0 {
					tmp.SetRGBA(x, y, color.RGBA{0, 0, 0, 0})
					changed++
				}
			}
		}
		copy(dst.Pix, tmp.Pix)
		if changed == 0 {
			break
		}
	}
}

// softSnapToLegend aligns colors to the legend conservatively:
// only when nearest palette class is unambiguous and locally supported.
func softSnapToLegend(dst *image.RGBA) {
	if dst == nil {
		return
	}
	b := dst.Bounds()
	w, h := b.Dx(), b.Dy()
	if w < 3 || h < 3 {
		return
	}
	tmp := image.NewRGBA(b)
	copy(tmp.Pix, dst.Pix)

	const (
		veryCloseDistSq = 14 * 14
		closeDistSq     = 24 * 24
		minGapSq        = 70
		minVotes        = 3
	)
	type key [3]uint8
	for y := 1; y < h-1; y++ {
		for x := 1; x < w-1; x++ {
			c := dst.RGBAAt(x, y)
			if c.A == 0 || !isUsefulPrecipPixel(c, 14) {
				continue
			}
			best, bestD, secondD := nearestPhenomenaColorWithDistance(c)
			if bestD > closeDistSq {
				continue
			}
			if bestD > veryCloseDistSq && (secondD-bestD) < minGapSq {
				continue
			}

			votes := 0
			for oy := -1; oy <= 1; oy++ {
				for ox := -1; ox <= 1; ox++ {
					if ox == 0 && oy == 0 {
						continue
					}
					n := dst.RGBAAt(x+ox, y+oy)
					if n.A == 0 || !isUsefulPrecipPixel(n, 14) {
						continue
					}
					nb, nd, _ := nearestPhenomenaColorWithDistance(n)
					if nd <= closeDistSq && (key{nb.R, nb.G, nb.B}) == (key{best.R, best.G, best.B}) {
						votes++
					}
				}
			}
			if bestD <= veryCloseDistSq || votes >= minVotes {
				tmp.SetRGBA(x, y, color.RGBA{R: best.R, G: best.G, B: best.B, A: 255})
			}
		}
	}
	copy(dst.Pix, tmp.Pix)
}

// temporalStabilizeFrames reduces one-frame flicker while preserving moving edges.
// It applies a conservative majority vote from previous/next frames on low-confidence pixels.
func temporalStabilizeFrames(frames []*image.RGBA, passes int) {
	if len(frames) < 3 || passes <= 0 {
		return
	}
	for pass := 0; pass < passes; pass++ {
		for i := 1; i < len(frames)-1; i++ {
			prev := frames[i-1]
			curr := frames[i]
			next := frames[i+1]
			if prev == nil || curr == nil || next == nil {
				continue
			}
			b := curr.Bounds()
			w, h := b.Dx(), b.Dy()
			if w < 3 || h < 3 {
				continue
			}
			tmp := image.NewRGBA(b)
			copy(tmp.Pix, curr.Pix)
			for y := 1; y < h-1; y++ {
				for x := 1; x < w-1; x++ {
					c := curr.RGBAAt(x, y)
					p := prev.RGBAAt(x, y)
					n := next.RGBAAt(x, y)

					// Kill isolated one-frame sparkles.
					if c.A > 0 && p.A == 0 && n.A == 0 && c.A < 165 {
						tmp.SetRGBA(x, y, color.RGBA{0, 0, 0, 0})
						continue
					}
					if p.A == 0 || n.A == 0 {
						continue
					}
					pc := nearestPhenomenaColor(p)
					nc := nearestPhenomenaColor(n)
					if pc != nc {
						continue
					}

					if c.A == 0 {
						// Fill only if neighbors strongly agree.
						seedAlpha := int(p.A) + int(n.A)
						if seedAlpha >= 290 {
							tmp.SetRGBA(x, y, color.RGBA{
								R: pc.R, G: pc.G, B: pc.B, A: uint8(minInt(230, seedAlpha/2)),
							})
						}
						continue
					}

					cc := nearestPhenomenaColor(c)
					if cc != pc && c.A < 205 {
						// Resolve uncertain class flips if both neighbors agree.
						tmp.SetRGBA(x, y, color.RGBA{
							R: pc.R,
							G: pc.G,
							B: pc.B,
							A: uint8(max(int(c.A), (int(p.A)+int(n.A))/2)),
						})
					}
				}
			}
			copy(curr.Pix, tmp.Pix)
		}
	}
}

func hasCarrierSupportRGBA(img *image.RGBA, x, y, radius, minHits int) bool {
	if img == nil {
		return false
	}
	b := img.Bounds()
	w, h := b.Dx(), b.Dy()
	hits := 0
	for oy := -radius; oy <= radius; oy++ {
		for ox := -radius; ox <= radius; ox++ {
			if ox == 0 && oy == 0 {
				continue
			}
			nx := x + ox
			ny := y + oy
			if nx < 0 || nx >= w || ny < 0 || ny >= h {
				continue
			}
			c := img.RGBAAt(nx+b.Min.X, ny+b.Min.Y)
			if c.A == 0 {
				continue
			}
			if isCarrierPrecipColor(c) {
				hits++
				if hits >= minHits {
					return true
				}
			}
		}
	}
	return false
}

func hasThunderSupportRGBA(img *image.RGBA, x, y, radius, minHits int) bool {
	if img == nil {
		return false
	}
	b := img.Bounds()
	w, h := b.Dx(), b.Dy()
	hits := 0
	for oy := -radius; oy <= radius; oy++ {
		for ox := -radius; ox <= radius; ox++ {
			if ox == 0 && oy == 0 {
				continue
			}
			nx := x + ox
			ny := y + oy
			if nx < 0 || nx >= w || ny < 0 || ny >= h {
				continue
			}
			c := img.RGBAAt(nx+b.Min.X, ny+b.Min.Y)
			if c.A == 0 {
				continue
			}
			idx, ok := legendClassIndex(c)
			if ok && isThunderLikeClass(idx) {
				hits++
				if hits >= minHits {
					return true
				}
			}
		}
	}
	return false
}

func hasTemporalSupportRGBA(prev, next *image.RGBA, x, y int) bool {
	check := func(img *image.RGBA) bool {
		if img == nil {
			return false
		}
		b := img.Bounds()
		w, h := b.Dx(), b.Dy()
		for oy := -1; oy <= 1; oy++ {
			for ox := -1; ox <= 1; ox++ {
				nx := x + ox
				ny := y + oy
				if nx < 0 || nx >= w || ny < 0 || ny >= h {
					continue
				}
				c := img.RGBAAt(nx+b.Min.X, ny+b.Min.Y)
				if c.A == 0 {
					continue
				}
				if isConvectiveCoreColor(c) || isCarrierPrecipColor(c) {
					return true
				}
			}
		}
		return false
	}
	return check(prev) || check(next)
}

// cleanupConvectiveSpeckles removes isolated yellow/brown convective artifacts
// that survived plus-removal, while preserving cores embedded in precipitation.
func cleanupConvectiveSpeckles(frames []*image.RGBA) {
	if len(frames) == 0 {
		return
	}
	for i := range frames {
		fr := frames[i]
		if fr == nil {
			continue
		}
		var prev *image.RGBA
		var next *image.RGBA
		if i > 0 {
			prev = frames[i-1]
		}
		if i+1 < len(frames) {
			next = frames[i+1]
		}
		b := fr.Bounds()
		w, h := b.Dx(), b.Dy()
		if w < 3 || h < 3 {
			continue
		}

		clearMask := make([]bool, w*h)
		// Pixel-level pre-clean: isolated convective pixels without carrier context.
		for y := 0; y < h; y++ {
			for x := 0; x < w; x++ {
				c := fr.RGBAAt(x+b.Min.X, y+b.Min.Y)
				if c.A == 0 || !isConvectiveCoreColor(c) {
					continue
				}
				idx, ok := legendClassIndex(c)
				hailLike := ok && isHailLikeClass(idx)
				hasCarrier := hasCarrierSupportRGBA(fr, x, y, 2, 3)
				hasThunder := hasThunderSupportRGBA(fr, x, y, 3, 2)
				if hasCarrier && (!hailLike || hasThunder) {
					continue
				}
				if hasTemporalSupportRGBA(prev, next, x, y) {
					continue
				}
				if hailLike && hasCarrier && hasThunder {
					continue
				}
				clearMask[y*w+x] = true
			}
		}

		visited := make([]bool, w*h)
		queue := make([]int, 0, 64)
		for idx := 0; idx < w*h; idx++ {
			if visited[idx] {
				continue
			}
			x0 := idx % w
			y0 := idx / w
			c0 := fr.RGBAAt(x0+b.Min.X, y0+b.Min.Y)
			if c0.A == 0 || !isConvectiveCoreColor(c0) {
				continue
			}
			queue = queue[:0]
			comp := make([]int, 0, 64)
			queue = append(queue, idx)
			visited[idx] = true
			carrierBorder := 0
			thunderBorder := 0
			temporalHits := 0
			hailHits := 0

			for qi := 0; qi < len(queue); qi++ {
				p := queue[qi]
				comp = append(comp, p)
				x := p % w
				y := p / w
				if hasCarrierSupportRGBA(fr, x, y, 1, 1) {
					carrierBorder++
				}
				if hasThunderSupportRGBA(fr, x, y, 2, 1) {
					thunderBorder++
				}
				if hasTemporalSupportRGBA(prev, next, x, y) {
					temporalHits++
				}
				cp := fr.RGBAAt(x+b.Min.X, y+b.Min.Y)
				if idxClass, ok := legendClassIndex(cp); ok && isHailLikeClass(idxClass) {
					hailHits++
				}
				if x > 0 {
					n := p - 1
					if !visited[n] {
						cn := fr.RGBAAt((n%w)+b.Min.X, (n/w)+b.Min.Y)
						if cn.A > 0 && isConvectiveCoreColor(cn) {
							visited[n] = true
							queue = append(queue, n)
						}
					}
				}
				if x+1 < w {
					n := p + 1
					if !visited[n] {
						cn := fr.RGBAAt((n%w)+b.Min.X, (n/w)+b.Min.Y)
						if cn.A > 0 && isConvectiveCoreColor(cn) {
							visited[n] = true
							queue = append(queue, n)
						}
					}
				}
				if y > 0 {
					n := p - w
					if !visited[n] {
						cn := fr.RGBAAt((n%w)+b.Min.X, (n/w)+b.Min.Y)
						if cn.A > 0 && isConvectiveCoreColor(cn) {
							visited[n] = true
							queue = append(queue, n)
						}
					}
				}
				if y+1 < h {
					n := p + w
					if !visited[n] {
						cn := fr.RGBAAt((n%w)+b.Min.X, (n/w)+b.Min.Y)
						if cn.A > 0 && isConvectiveCoreColor(cn) {
							visited[n] = true
							queue = append(queue, n)
						}
					}
				}
			}

			area := len(comp)
			if area == 0 {
				continue
			}
			remove := false
			if area <= 8 && carrierBorder == 0 && temporalHits == 0 {
				remove = true
			}
			if area <= 28 && carrierBorder < max(2, area/3) && temporalHits < max(2, area/5) {
				remove = true
			}
			hailDominant := hailHits*2 >= area
			if hailDominant {
				// Градовые цвета без грозового контекста и temporal-поддержки считаем мусором.
				if area <= 64 && thunderBorder < max(2, area/6) && temporalHits < max(2, area/5) {
					remove = true
				}
				// Даже в больших компонентах удаляем "сухой" желто-коричневый шум.
				if thunderBorder == 0 && carrierBorder < max(2, area/5) && temporalHits == 0 {
					remove = true
				}
			}
			if remove {
				for _, p := range comp {
					clearMask[p] = true
				}
			}
		}

		for y := 0; y < h; y++ {
			for x := 0; x < w; x++ {
				if clearMask[y*w+x] {
					fr.SetRGBA(x+b.Min.X, y+b.Min.Y, color.RGBA{0, 0, 0, 0})
				}
			}
		}
	}
}

func computeFrameBatchMetrics(frames []*image.RGBA) frameBatchMetrics {
	var out frameBatchMetrics
	out.FrameCount = len(frames)
	if len(frames) == 0 {
		return out
	}

	var coverageSum float64
	var alphaSum float64
	var alphaCount int
	var boundaryEdges int
	var boundaryChecks int
	var flickerDiff int
	var flickerChecks int

	for i, fr := range frames {
		if fr == nil {
			continue
		}
		b := fr.Bounds()
		w, h := b.Dx(), b.Dy()
		if w == 0 || h == 0 {
			continue
		}
		nonTransparent := 0
		for y := 0; y < h; y++ {
			for x := 0; x < w; x++ {
				c := fr.RGBAAt(x, y)
				if c.A > 0 {
					nonTransparent++
					alphaSum += float64(c.A)
					alphaCount++
				}
				if x+1 < w {
					r := fr.RGBAAt(x+1, y)
					if (c.A > 0) != (r.A > 0) {
						boundaryEdges++
					}
					boundaryChecks++
				}
				if y+1 < h {
					d := fr.RGBAAt(x, y+1)
					if (c.A > 0) != (d.A > 0) {
						boundaryEdges++
					}
					boundaryChecks++
				}
			}
		}
		coverageSum += float64(nonTransparent) / float64(w*h)

		if i > 0 && frames[i-1] != nil {
			prev := frames[i-1]
			bp := prev.Bounds()
			wp, hp := bp.Dx(), bp.Dy()
			if wp == w && hp == h {
				for y := 0; y < h; y++ {
					for x := 0; x < w; x++ {
						a0 := prev.RGBAAt(x, y).A
						a1 := fr.RGBAAt(x, y).A
						if (a0 > 0) != (a1 > 0) {
							flickerDiff++
						}
						flickerChecks++
					}
				}
				out.TemporalPairs++
			}
		}
	}

	if len(frames) > 0 {
		out.CoverageMean = coverageSum / float64(len(frames))
	}
	if flickerChecks > 0 {
		out.FlickerRate = float64(flickerDiff) / float64(flickerChecks)
	}
	if boundaryChecks > 0 {
		out.EdgeRoughness = float64(boundaryEdges) / float64(boundaryChecks)
	}
	if alphaCount > 0 {
		out.MeanAlpha = alphaSum / float64(alphaCount)
	}
	out.PixelSamples = flickerChecks
	out.BoundaryChecks = boundaryChecks
	return out
}

func savePNG(path string, img image.Image) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return png.Encode(f, img)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}

func hashRGBA(img *image.RGBA) uint64 {
	h := fnv.New64a()
	var b [8]byte
	binary.LittleEndian.PutUint32(b[:4], uint32(img.Bounds().Dx()))
	binary.LittleEndian.PutUint32(b[4:], uint32(img.Bounds().Dy()))
	_, _ = h.Write(b[:])
	_, _ = h.Write(img.Pix)
	return h.Sum64()
}

func padInt(n, width int) string {
	s := fmt.Sprintf("%d", n)
	for len(s) < width {
		s = "0" + s
	}
	return s
}
