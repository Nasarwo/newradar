const FRAME_FAST_INTERVAL_MS = 150;
const FRAME_LAST_HOLD_MS = 2200;
const SATELLITE_FRAME_FAST_INTERVAL_MS = 500;
const SATELLITE_FRAME_LAST_HOLD_MS = 2700;
const DATA_REFRESH_MS = 60_000;
const NOWCAST_WMS_VERSION = "1.3.0";
const SATELLITE_FRAME_LIMIT = 18;
const SATELLITE_CADENCE_MIN = 10;
const SOURCE_HD = "__hd__";
const SOURCE_SATELLITE = "__satellite__";
const SATELLITE_REFERENCE_TILE_URL =
  "https://services.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places_Alternate/MapServer/tile/{z}/{y}/{x}";
const NOWCAST_LAYER_LABELS = {
  bufr_phenomena: "Опасные явления",
  bufr_height: "Высота ВГО",
  bufr_dbz1: "Отражаемость (dbz)",
  bufr_precip: "Интенсивность осадков",
};
const NOWCAST_LAYER_ORDER = [
  "bufr_phenomena",
  "bufr_height",
  "bufr_dbz1",
  "bufr_precip",
];
const SATELLITE_LAYER_LABEL = "Спутник HD";
const SATELLITE_WMTS_LAYER_CANDIDATES = [
  "GOES-East_ABI_Band13_Clean_Infrared",
  "GOES-West_ABI_Band13_Clean_Infrared",
  "GOES-East_ABI_Air_Mass",
];
const SATELLITE_WMTS_MATRIX_CANDIDATES = [
  "GoogleMapsCompatible_Level8",
  "GoogleMapsCompatible_Level7",
  "GoogleMapsCompatible_Level6",
];
const HD_LAYER_OPACITY = 179 / 255;
const NOWCAST_LAYER_OPACITY = 1;
const TRANSPARENT_PIXEL =
  "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==";
// Резервный extent для кадров осадков (Европа/РФ), если API не вернёт geoBounds
const DEFAULT_GEO_BOUNDS = [20, 40, 70, 66]; // [west, south, east, north] WGS84

const statusEl = document.getElementById("status");
const timeLabelEl = document.getElementById("timeLabel");
const timelineEl = document.getElementById("timeline");
const mobileMenuToggleBtnEl = document.getElementById("mobileMenuToggleBtn");
const mobileMenuBackdropEl = document.getElementById("mobileMenuBackdrop");
const mobileMenuDrawerEl = document.getElementById("mobileMenuDrawer");
const mobileMenuCloseBtnEl = document.getElementById("mobileMenuCloseBtn");
const playBtnEl = document.getElementById("playBtn");
const firstBtnEl = document.getElementById("firstBtn");
const prevBtnEl = document.getElementById("prevBtn");
const nextBtnEl = document.getElementById("nextBtn");
const lastBtnEl = document.getElementById("lastBtn");
const measureBtnEl = document.getElementById("measureBtn");
const drawBtnEl = document.getElementById("drawBtn");
const crosshairBtnEl = document.getElementById("crosshairBtn");
const lightningToggleBtnEl = document.getElementById("lightningToggleBtn");
const playbackFrameCountEl = document.getElementById("playbackFrameCount");
const sourceLayerSelectEl = document.getElementById("sourceLayerSelect");
const mobileSourceLayerSelectEl = document.getElementById(
  "mobileSourceLayerSelect",
);
const mobileSatelliteStylePanelEl = document.getElementById(
  "mobileSatelliteStylePanel",
);
const mobileSatelliteColorToggleEl = document.getElementById(
  "mobileSatelliteColorToggle",
);
const satelliteStylePanelEl = document.getElementById("satelliteStylePanel");
const satelliteColorModeEl = document.getElementById("satelliteColorMode");
const legendPanelEl = document.getElementById("legendPanel");
const legendToggleEl = document.getElementById("legendToggle");
const legendTitleTextEl = document.getElementById("legendTitleText");
const legendRowsEl = document.getElementById("legendRows");
const mobileLegendPanelEl = document.getElementById("mobileLegendPanel");
const mobileLegendTitleTextEl = document.getElementById(
  "mobileLegendTitleText",
);
const mobileLegendRowsEl = document.getElementById("mobileLegendRows");
const mapCrosshairEl = document.getElementById("mapCrosshair");
const crosshairInfoEl = document.getElementById("crosshairInfo");
const lightningPopupEl = document.getElementById("lightningPopup");

let map;
let baseLayer;
let frameLayer;
let satelliteLayer;
let radarLayer;
let satelliteReferenceLayer;
let measureLayer;
let drawLayer;
let lightningLayer;
let lightningPopupOverlay;
let frames = [];
let hdFrames = [];
let activeSource = SOURCE_HD;
let satelliteResolvedLayer = "";
let radarImageSize = [0, 0];
let nowcastMeta = null;
const nowcastFramesByLayer = new Map();
const satelliteFramesByKey = new Map();
let geoBounds = DEFAULT_GEO_BOUNDS.slice();
let geoBounds3857 = null;
let radars = [];
let currentFrame = 0;
let timer = null;
let isPlaying = false;
let playbackGeneration = 0;
let dataRefreshTimer = null;
let measureMode = false;
let drawMode = false;
let crosshairMode = false;
let lightningOverlayEnabled = true;
let lightningRequestToken = 0;
let measureStart = null;
let measureEnd = null;
let measurePointer = null;
let drawCurrentStroke = null;
let drawCurrentStrokeFeature = null;
let satelliteWMTSConfig = null;
const satelliteSourceCache = new Map();
let sourceSwitchToken = 0;
const PHENOMENA_LEGEND = [
  { label: "Обл. сред. яруса", rgb: [156, 170, 179] },
  { label: "Сл. образования", rgb: [162, 198, 254] },
  { label: "Осадки слабые", rgb: [70, 254, 149] },
  { label: "Осадки умеренные", rgb: [1, 194, 94] },
  { label: "Осадки сильные", rgb: [1, 154, 8] },
  { label: "Кучевая обл", rgb: [255, 255, 131] },
  { label: "Ливень слабый", rgb: [62, 137, 253] },
  { label: "Ливень умеренный", rgb: [1, 58, 255] },
  { label: "Ливень сильный", rgb: [2, 8, 119] },
  { label: "Гроза (R)", rgb: [255, 171, 128] },
  { label: "Гроза R", rgb: [255, 89, 132] },
  { label: "Гроза R+", rgb: [253, 6, 9] },
  { label: "Град слабый", rgb: [205, 105, 8] },
  { label: "Град умеренный", rgb: [143, 73, 15] },
  { label: "Град сильный", rgb: [88, 14, 8] },
  { label: "Шквал слабый", rgb: [255, 171, 255] },
  { label: "Шквал умеренный", rgb: [255, 88, 255] },
  { label: "Шквал сильный", rgb: [200, 9, 202] },
  { label: "Смерч", rgb: [47, 49, 73] },
];
const HEIGHT_LEGEND = [
  { label: "0.00 км", rgb: [112, 232, 179] },
  { label: "0.20 км", rgb: [112, 232, 183] },
  { label: "0.50 км", rgb: [104, 214, 176] },
  { label: "1.00 км", rgb: [96, 195, 166] },
  { label: "2.00 км", rgb: [90, 163, 149] },
  { label: "3.00 км", rgb: [82, 125, 119] },
  { label: "4.00 км", rgb: [108, 211, 237] },
  { label: "5.00 км", rgb: [96, 184, 234] },
  { label: "6.00 км", rgb: [88, 146, 226] },
  { label: "7.00 км", rgb: [86, 96, 182] },
  { label: "8.00 км", rgb: [214, 200, 93] },
  { label: "9.00 км", rgb: [218, 112, 77] },
  { label: "10.00 км", rgb: [214, 88, 83] },
  { label: "11.00 км", rgb: [154, 78, 84] },
  { label: "12.00 км", rgb: [120, 190, 94] },
  { label: "13.00 км", rgb: [80, 146, 86] },
  { label: "14.00 км", rgb: [206, 95, 153] },
  { label: "15.00 км", rgb: [216, 110, 192] },
];
const DBZ_LEGEND = [
  { label: "-30 dBZ", rgb: [215, 215, 215] },
  { label: "-10 dBZ", rgb: [200, 200, 200] },
  { label: "-5 dBZ", rgb: [179, 210, 234] },
  { label: "0 dBZ", rgb: [182, 227, 146] },
  { label: "5 dBZ", rgb: [120, 235, 114] },
  { label: "10 dBZ", rgb: [95, 184, 234] },
  { label: "15 dBZ", rgb: [76, 120, 232] },
  { label: "20 dBZ", rgb: [75, 72, 215] },
  { label: "25 dBZ", rgb: [74, 71, 174] },
  { label: "30 dBZ", rgb: [231, 233, 92] },
  { label: "35 dBZ", rgb: [244, 180, 95] },
  { label: "40 dBZ", rgb: [248, 118, 113] },
  { label: "45 dBZ", rgb: [249, 88, 88] },
  { label: "50 dBZ", rgb: [114, 212, 99] },
  { label: "55 dBZ", rgb: [84, 182, 83] },
  { label: "60 dBZ", rgb: [225, 78, 235] },
  { label: "65 dBZ", rgb: [201, 65, 223] },
  { label: "70 dBZ", rgb: [142, 76, 76] },
];
const PRECIP_LEGEND = [
  { label: "0.10 мм/ч", rgb: [185, 185, 185] },
  { label: "0.30 мм/ч", rgb: [143, 143, 143] },
  { label: "0.50 мм/ч", rgb: [90, 135, 232] },
  { label: "1.00 мм/ч", rgb: [76, 75, 169] },
  { label: "3.00 мм/ч", rgb: [236, 234, 99] },
  { label: "5.00 мм/ч", rgb: [205, 232, 92] },
  { label: "7.00 мм/ч", rgb: [242, 177, 92] },
  { label: "10.00 мм/ч", rgb: [247, 119, 88] },
  { label: "20.00 мм/ч", rgb: [249, 92, 87] },
  { label: "30.00 мм/ч", rgb: [150, 230, 154] },
  { label: "50.00 мм/ч", rgb: [100, 185, 92] },
  { label: "100.00 мм/ч", rgb: [223, 155, 224] },
  { label: ">100 мм/ч", rgb: [203, 79, 209] },
];
const LEGEND_CONFIG_BY_SOURCE = {
  [SOURCE_HD]: {
    title: "Опасные явления HD",
    items: PHENOMENA_LEGEND,
    maxDist: 70,
  },
  bufr_phenomena: {
    title: "Опасные явления",
    items: PHENOMENA_LEGEND,
    maxDist: 70,
  },
  bufr_height: {
    title: "Высота ВГО",
    items: HEIGHT_LEGEND,
    maxDist: 70,
    forceNearest: true,
  },
  bufr_dbz1: {
    title: "Отражаемость (dBZ)",
    items: DBZ_LEGEND,
    maxDist: 70,
    forceNearest: true,
  },
  bufr_precip: {
    title: "Интенсивность",
    items: PRECIP_LEGEND,
    maxDist: 70,
    forceNearest: true,
  },
};
const PHENOMENA_MATCH_MAX_DIST = 70;
const SHOWER_MATCH_MAX_DIST = 130;
const framePixelCache = new Map();
const frameImageSizeCache = new Map();
const frameStyledUrlCache = new Map();
const RADAR_PIXEL_CUT_STYLE = false;
const RADAR_PIXEL_CUT_STEP = 2;
const RADAR_PIXEL_CUT_ALPHA = 0.62;
const DISPLAY_SOFT_PALETTE_SNAP = false;
const DISPLAY_SNAP_MAX_DIST_SQ = 26 * 26;
const DISPLAY_SNAP_MIN_GAP_SQ = 80;
const NOWCAST_PREFETCH_MAX_FRAMES = 72;
const NOWCAST_RADAR_BG_RGB = [0xd3, 0xd8, 0xd8];
const NOWCAST_RADAR_BG_TOLERANCE = 20;
const NOWCAST_GRAY_BG_RGB = [202, 202, 202];
const NOWCAST_GRAY_BG_TOLERANCE = 34;
const NOWCAST_COLOR_ALPHA = 0.86;
const NOWCAST_EDGE_CUT_STYLE = false;
const NOWCAST_EDGE_CUT_ALPHA = 0.8;
const NOWCAST_EDGE_CUT_BRIGHTEN = 12;
const NOWCAST_EDGE_COLOR_DIFF = 42;
const NOWCAST_PIXEL_GAP_OVERLAY = true;
const NOWCAST_PIXEL_GAP_MIN_SCREEN_PX = 3;
const NOWCAST_PIXEL_GAP_MAX_SCREEN_PX = 1.25;
let frameRenderToken = 0;
let centerProbeToken = 0;
let viewportSyncTimer = null;
const nowcastBlobUrlCache = new Map();
const nowcastBlobUrlOrder = [];
const satelliteBlobUrlCache = new Map();
const satelliteBlobUrlOrder = [];
const satelliteTileStyleCache = new Map();
const satelliteTileStyleOrder = [];
let satelliteRadiationColorEnabled = false;

function updateSatelliteStylePanelVisibility() {
  satelliteStylePanelEl?.classList.toggle(
    "hidden",
    activeSource !== SOURCE_SATELLITE,
  );
  mobileSatelliteStylePanelEl?.classList.toggle(
    "hidden",
    activeSource !== SOURCE_SATELLITE,
  );
}

function syncSatelliteModeControls() {
  const mode = satelliteRadiationColorEnabled ? "cloudtop" : "rgb";
  if (satelliteColorModeEl) satelliteColorModeEl.value = mode;
  if (mobileSatelliteColorToggleEl) {
    mobileSatelliteColorToggleEl.checked = satelliteRadiationColorEnabled;
  }
}

async function applySatelliteMode(enabled) {
  const nextEnabled = Boolean(enabled);
  if (nextEnabled === satelliteRadiationColorEnabled) {
    syncSatelliteModeControls();
    return;
  }
  satelliteRadiationColorEnabled = nextEnabled;
  syncSatelliteModeControls();
  clearSatelliteStyleCaches();
  if (activeSource !== SOURCE_SATELLITE) return;
  satelliteLayer?.setSource(null);
  setStatus("Применение спутникового режима...");
  try {
    await switchSource(SOURCE_SATELLITE);
  } catch (e) {
    console.warn("Satellite style switch failed:", e);
    setStatus(`Ошибка переключения спутникового режима: ${e.message}`);
  }
}

function getSatelliteStyleCacheKey(sourceUrl) {
  const mode = satelliteRadiationColorEnabled ? "cloudtop" : "rgb";
  return `${mode}|${sourceUrl}`;
}

function clearSatelliteStyleCaches() {
  for (const url of satelliteBlobUrlCache.values()) {
    URL.revokeObjectURL(url);
  }
  satelliteBlobUrlCache.clear();
  satelliteBlobUrlOrder.length = 0;

  for (const url of satelliteTileStyleCache.values()) {
    URL.revokeObjectURL(url);
  }
  satelliteTileStyleCache.clear();
  satelliteTileStyleOrder.length = 0;
  satelliteSourceCache.clear();
  satelliteFramesByKey.clear();
}

function getNearestLegendColorInfo(r, g, b) {
  let best = PHENOMENA_LEGEND[0];
  let bestDist = Infinity;
  let secondDist = Infinity;
  for (const p of PHENOMENA_LEGEND) {
    const dr = r - p.rgb[0];
    const dg = g - p.rgb[1];
    const db = b - p.rgb[2];
    const dist = dr * dr + dg * dg + db * db;
    if (dist < bestDist) {
      secondDist = bestDist;
      bestDist = dist;
      best = p;
    } else if (dist < secondDist) {
      secondDist = dist;
    }
  }
  return { best, bestDist, secondDist };
}

function setStatus(text) {
  if (statusEl) statusEl.textContent = text;
}

function setPrecipInfo(text) {
  if (crosshairInfoEl) crosshairInfoEl.textContent = text;
}

function isMobileLayout() {
  return window.matchMedia("(max-width: 640px)").matches;
}

function setSourceSelectValue(value) {
  const next = String(value || SOURCE_HD);
  const hasDesktop = Array.from(sourceLayerSelectEl?.options || []).some(
    (opt) => opt.value === next,
  );
  const hasMobile = Array.from(mobileSourceLayerSelectEl?.options || []).some(
    (opt) => opt.value === next,
  );
  if (hasDesktop && sourceLayerSelectEl) sourceLayerSelectEl.value = next;
  if (hasMobile && mobileSourceLayerSelectEl)
    mobileSourceLayerSelectEl.value = next;
}

function setMobileMenuOpen(open) {
  if (!mobileMenuDrawerEl || !mobileMenuBackdropEl) return;
  const isOpen = Boolean(open);
  mobileMenuDrawerEl.classList.toggle("hidden", !isOpen);
  mobileMenuBackdropEl.classList.toggle("hidden", !isOpen);
  mobileMenuDrawerEl.classList.toggle("open", isOpen);
  mobileMenuDrawerEl.setAttribute("aria-hidden", String(!isOpen));
  mobileMenuToggleBtnEl?.setAttribute("aria-expanded", String(isOpen));
}

function setCrosshairMode(enabled) {
  const supported = activeSource !== SOURCE_SATELLITE;
  crosshairMode = supported && Boolean(enabled);
  if (crosshairBtnEl) crosshairBtnEl.classList.toggle("active", crosshairMode);
  if (crosshairBtnEl) crosshairBtnEl.disabled = !supported;
  if (mapCrosshairEl) mapCrosshairEl.classList.toggle("hidden", !crosshairMode);
  if (crosshairInfoEl)
    crosshairInfoEl.classList.toggle("hidden", !crosshairMode || !supported);
  if (crosshairMode) {
    inspectPrecipAtCrosshairCenter().catch(() => {});
  }
}

function getSourceDisplayName(sourceKey) {
  if (sourceKey === SOURCE_HD) return "Опасные явления HD";
  if (sourceKey === SOURCE_SATELLITE) return SATELLITE_LAYER_LABEL;
  return NOWCAST_LAYER_LABELS[sourceKey] || sourceKey;
}

function getLegendConfigForSource(sourceKey) {
  return (
    LEGEND_CONFIG_BY_SOURCE[sourceKey] || LEGEND_CONFIG_BY_SOURCE[SOURCE_HD]
  );
}

function renderLegendRows(rowsEl, items) {
  if (!rowsEl) return;
  rowsEl.innerHTML = "";
  for (const item of items) {
    const row = document.createElement("tr");
    const colorCell = document.createElement("td");
    const textCell = document.createElement("td");
    const swatch = document.createElement("span");
    swatch.className = "palette-color";
    swatch.style.background = `rgb(${item.rgb[0]}, ${item.rgb[1]}, ${item.rgb[2]})`;
    colorCell.appendChild(swatch);
    textCell.textContent = item.label;
    row.appendChild(colorCell);
    row.appendChild(textCell);
    rowsEl.appendChild(row);
  }
}

function renderActiveLegend() {
  if (activeSource === SOURCE_SATELLITE) {
    legendPanelEl?.classList.add("hidden");
    mobileLegendPanelEl?.classList.add("hidden");
    return;
  }
  legendPanelEl?.classList.remove("hidden");
  mobileLegendPanelEl?.classList.remove("hidden");

  const cfg = getLegendConfigForSource(activeSource);
  if (legendTitleTextEl) legendTitleTextEl.textContent = `Легенда`;
  if (mobileLegendTitleTextEl) mobileLegendTitleTextEl.textContent = "Легенда";
  renderLegendRows(legendRowsEl, cfg.items);
  renderLegendRows(mobileLegendRowsEl, cfg.items);
}

function clamp01(v) {
  if (v < 0) return 0;
  if (v > 1) return 1;
  return v;
}

function lerp(a, b, t) {
  return Math.round(a + (b - a) * t);
}

function lerpColor(c1, c2, t) {
  return [lerp(c1[0], c2[0], t), lerp(c1[1], c2[1], t), lerp(c1[2], c2[2], t)];
}

function pseudoBtCByLuma(luma) {
  // Для большинства IR-изображений в этой ленте: чем пиксель светлее,
  // тем холоднее верхняя граница облака.
  const n = Math.pow(clamp01(luma / 255), 0.92);
  return 50 - n * 140;
}

const SATELLITE_CTA_STOPS = [
  { t: -90, rgb: [38, 6, 34] },
  { t: -82, rgb: [123, 12, 12] },
  { t: -74, rgb: [209, 22, 12] },
  { t: -66, rgb: [252, 87, 8] },
  { t: -60, rgb: [255, 156, 16] },
  { t: -56, rgb: [251, 212, 22] },
  { t: -52, rgb: [201, 243, 63] },
  { t: -48, rgb: [126, 236, 96] },
  { t: -44, rgb: [52, 214, 180] },
  { t: -40, rgb: [9, 160, 255] },
  { t: -36, rgb: [10, 103, 255] },
  { t: -32, rgb: [18, 54, 219] },
  { t: -24, rgb: [128, 128, 128] },
  { t: -12, rgb: [164, 164, 164] },
  { t: 0, rgb: [198, 198, 198] },
  { t: 12, rgb: [150, 150, 150] },
  { t: 30, rgb: [88, 88, 88] },
  { t: 50, rgb: [22, 22, 22] },
];

const SATELLITE_METEOLOGIX_STOPS = [
  { t: -92, rgb: [255, 254, 210] },
  { t: -82, rgb: [255, 248, 174] },
  { t: -72, rgb: [250, 236, 132] },
  { t: -62, rgb: [240, 220, 114] },
  { t: -52, rgb: [221, 204, 132] },
  { t: -42, rgb: [201, 195, 166] },
  { t: -30, rgb: [176, 177, 182] },
  { t: -18, rgb: [151, 154, 164] },
  { t: -6, rgb: [124, 130, 146] },
  { t: 8, rgb: [98, 106, 124] },
  { t: 24, rgb: [76, 84, 103] },
  { t: 50, rgb: [56, 62, 82] },
];

function ctaLikeRgbByBt(bt) {
  if (bt <= SATELLITE_CTA_STOPS[0].t) return SATELLITE_CTA_STOPS[0].rgb;
  const last = SATELLITE_CTA_STOPS[SATELLITE_CTA_STOPS.length - 1];
  if (bt >= last.t) return last.rgb;
  for (let i = 1; i < SATELLITE_CTA_STOPS.length; i++) {
    const prev = SATELLITE_CTA_STOPS[i - 1];
    const next = SATELLITE_CTA_STOPS[i];
    if (bt > next.t) continue;
    const k = clamp01((bt - prev.t) / (next.t - prev.t));
    return lerpColor(prev.rgb, next.rgb, k);
  }
  return last.rgb;
}

function meteologixLikeRgbByBt(bt) {
  if (bt <= SATELLITE_METEOLOGIX_STOPS[0].t) {
    return SATELLITE_METEOLOGIX_STOPS[0].rgb;
  }
  const last =
    SATELLITE_METEOLOGIX_STOPS[SATELLITE_METEOLOGIX_STOPS.length - 1];
  if (bt >= last.t) return last.rgb;
  for (let i = 1; i < SATELLITE_METEOLOGIX_STOPS.length; i++) {
    const prev = SATELLITE_METEOLOGIX_STOPS[i - 1];
    const next = SATELLITE_METEOLOGIX_STOPS[i];
    if (bt > next.t) continue;
    const k = clamp01((bt - prev.t) / (next.t - prev.t));
    return lerpColor(prev.rgb, next.rgb, k);
  }
  return last.rgb;
}

function estimateCloudTopHeightKmByBt(btC) {
  // Более физичная и плавная оценка по стандартной атмосфере (ISA):
  // тропосфера ~6.5 C/км до тропопаузы (~11 км, -56.5 C),
  // выше — консервативное наращивание (ИК-температура не равна T воздуха 1:1).
  const bt = Number(btC);
  if (!Number.isFinite(bt)) return 0;
  if (bt >= 15) return 0;
  if (bt >= -56.5) {
    return Math.max(0, (15 - bt) / 6.5);
  }
  if (bt >= -70) {
    return 11 + ((-56.5 - bt) / 13.5) * 1.8; // 11..12.8 км
  }
  if (bt >= -80) {
    return 12.8 + ((-70 - bt) / 10) * 1.4; // 12.8..14.2 км
  }
  if (bt >= -90) {
    return 14.2 + ((-80 - bt) / 10) * 1.3; // 14.2..15.5 км
  }
  return 15.5;
}

function estimateBtBySatelliteColor(r, g, b) {
  const c = [r, g, b];
  let bestBt = SATELLITE_CTA_STOPS[0].t;
  let bestDist = Infinity;
  for (let i = 1; i < SATELLITE_CTA_STOPS.length; i++) {
    const prev = SATELLITE_CTA_STOPS[i - 1];
    const next = SATELLITE_CTA_STOPS[i];
    const seg = [
      next.rgb[0] - prev.rgb[0],
      next.rgb[1] - prev.rgb[1],
      next.rgb[2] - prev.rgb[2],
    ];
    const segLen2 = seg[0] * seg[0] + seg[1] * seg[1] + seg[2] * seg[2];
    let t = 0;
    if (segLen2 > 0) {
      const rel = [c[0] - prev.rgb[0], c[1] - prev.rgb[1], c[2] - prev.rgb[2]];
      t = clamp01(
        (rel[0] * seg[0] + rel[1] * seg[1] + rel[2] * seg[2]) / segLen2,
      );
    }
    const pr = prev.rgb[0] + seg[0] * t;
    const pg = prev.rgb[1] + seg[1] * t;
    const pb = prev.rgb[2] + seg[2] * t;
    const dr = c[0] - pr;
    const dg = c[1] - pg;
    const db = c[2] - pb;
    const dist = dr * dr + dg * dg + db * db;
    if (dist < bestDist) {
      bestDist = dist;
      bestBt = prev.t + (next.t - prev.t) * t;
    }
  }
  return bestBt;
}

async function styleSatelliteTileCTA(blob) {
  if (!satelliteRadiationColorEnabled) {
    return blob;
  }
  const canvas = document.createElement("canvas");
  const ctx = canvas.getContext("2d", { willReadFrequently: true });
  if (typeof createImageBitmap === "function") {
    const bitmap = await createImageBitmap(blob);
    canvas.width = bitmap.width;
    canvas.height = bitmap.height;
    ctx.drawImage(bitmap, 0, 0);
    bitmap.close();
  } else {
    const imageUrl = URL.createObjectURL(blob);
    const image = await new Promise((resolve, reject) => {
      const img = new Image();
      img.onload = () => resolve(img);
      img.onerror = () => reject(new Error("Satellite tile decode failed"));
      img.src = imageUrl;
    });
    URL.revokeObjectURL(imageUrl);
    canvas.width = image.width;
    canvas.height = image.height;
    ctx.drawImage(image, 0, 0);
  }

  const imageData = ctx.getImageData(0, 0, canvas.width, canvas.height);
  const data = imageData.data;
  for (let i = 0; i < data.length; i += 4) {
    const a = data[i + 3];
    if (a === 0) continue;
    const r = data[i];
    const g = data[i + 1];
    const b = data[i + 2];
    const luma = 0.2126 * r + 0.7152 * g + 0.0722 * b;
    const bt = pseudoBtCByLuma(luma);
    const [nr, ng, nb] = satelliteRadiationColorEnabled
      ? ctaLikeRgbByBt(bt)
      : meteologixLikeRgbByBt(bt);
    const coldNorm = Math.pow(clamp01((-32 - bt) / 48), 0.9);
    data[i] = nr;
    data[i + 1] = ng;
    data[i + 2] = nb;
    const alphaBase = satelliteRadiationColorEnabled ? 0.72 : 0.75;
    const alphaBoost = satelliteRadiationColorEnabled ? 0.24 : 0.2;
    data[i + 3] = Math.max(
      1,
      Math.round(a * (alphaBase + alphaBoost * coldNorm)),
    );
  }
  ctx.putImageData(imageData, 0, 0);
  return await new Promise((resolve, reject) => {
    canvas.toBlob(
      (out) => {
        if (!out) {
          reject(new Error("Satellite CTA style render failed"));
          return;
        }
        resolve(out);
      },
      "image/png",
      1,
    );
  });
}

function floorToTenMinutesUTC(date) {
  const d = new Date(date.getTime());
  d.setUTCMinutes(Math.floor(d.getUTCMinutes() / 10) * 10, 0, 0);
  return d;
}

function getRecentSatelliteTimes(
  limit = SATELLITE_FRAME_LIMIT,
  stepMinutes = 10,
) {
  const out = [];
  const now = floorToTenMinutesUTC(new Date());
  for (let i = limit - 1; i >= 0; i--) {
    out.push(new Date(now.getTime() - i * stepMinutes * 60 * 1000));
  }
  return out;
}

function formatSatelliteWMTSTime(d) {
  const iso = new Date(d).toISOString();
  return iso.slice(0, 19) + "Z";
}

function buildSatelliteWMTSURL(layerId, matrixSet, timeIso) {
  return `https://gibs.earthdata.nasa.gov/wmts/epsg3857/best/${layerId}/default/${timeIso}/${matrixSet}/{z}/{y}/{x}.png`;
}

async function isWorkingSatelliteWMTS(layerId, matrixSet, timeIso) {
  const sampleUrl = buildSatelliteWMTSURL(layerId, matrixSet, timeIso)
    .replace("{z}", "2")
    .replace("{y}", "1")
    .replace("{x}", "1");
  try {
    const response = await fetch(sampleUrl, {
      cache: "no-store",
      mode: "cors",
      credentials: "omit",
    });
    if (!response.ok) return false;
    const contentType = (
      response.headers.get("content-type") || ""
    ).toLowerCase();
    return contentType.includes("image/");
  } catch {
    return false;
  }
}

async function ensureSatelliteWMTSConfig() {
  if (satelliteWMTSConfig) return satelliteWMTSConfig;
  const timeCandidates = getRecentSatelliteTimes(36, SATELLITE_CADENCE_MIN)
    .reverse()
    .map((d) => formatSatelliteWMTSTime(d));
  for (const layerId of SATELLITE_WMTS_LAYER_CANDIDATES) {
    for (const matrixSet of SATELLITE_WMTS_MATRIX_CANDIDATES) {
      for (const timeIso of timeCandidates) {
        const ok = await isWorkingSatelliteWMTS(layerId, matrixSet, timeIso);
        if (!ok) continue;
        satelliteWMTSConfig = { layerId, matrixSet };
        return satelliteWMTSConfig;
      }
    }
  }
  return null;
}

function getSatelliteTileSource(urlTemplate) {
  if (satelliteSourceCache.has(urlTemplate)) {
    return satelliteSourceCache.get(urlTemplate);
  }
  const source = new ol.source.XYZ({
    url: urlTemplate,
    crossOrigin: "anonymous",
    wrapX: false,
    tileSize: 256,
    interpolate: true,
    tileLoadFunction: async (tile, src) => {
      const img = tile.getImage();
      const styleCacheKey = getSatelliteStyleCacheKey(src);
      const cached = satelliteTileStyleCache.get(styleCacheKey);
      if (cached) {
        img.src = cached;
        return;
      }
      try {
        const response = await fetch(src, {
          cache: "force-cache",
          mode: "cors",
        });
        if (!response.ok) {
          img.src = src;
          return;
        }
        const blob = await response.blob();
        const styled = await styleSatelliteTileCTA(blob);
        const styledUrl = URL.createObjectURL(styled);
        satelliteTileStyleCache.set(styleCacheKey, styledUrl);
        satelliteTileStyleOrder.push(styleCacheKey);
        while (satelliteTileStyleOrder.length > 800) {
          const oldKey = satelliteTileStyleOrder.shift();
          if (!oldKey) break;
          const oldUrl = satelliteTileStyleCache.get(oldKey);
          if (oldUrl) {
            URL.revokeObjectURL(oldUrl);
            satelliteTileStyleCache.delete(oldKey);
          }
        }
        img.src = styledUrl;
      } catch {
        img.src = src;
      }
    },
  });
  satelliteSourceCache.set(urlTemplate, source);
  return source;
}

function setPlaybackControlsEnabled(enabled) {
  const state = !enabled;
  if (playBtnEl) playBtnEl.disabled = state;
  if (firstBtnEl) firstBtnEl.disabled = state;
  if (prevBtnEl) prevBtnEl.disabled = state;
  if (nextBtnEl) nextBtnEl.disabled = state;
  if (lastBtnEl) lastBtnEl.disabled = state;
  if (timelineEl) timelineEl.disabled = state;
  if (playbackFrameCountEl) playbackFrameCountEl.disabled = state;
}

function updatePlayButton() {
  playBtnEl.textContent = isPlaying ? "⏸" : "▶";
  playBtnEl.classList.toggle("paused", !isPlaying);
}

function getPlaybackWindowCount() {
  if (!playbackFrameCountEl) return frames.length;
  const raw = playbackFrameCountEl.value;
  if (raw === "all") return frames.length;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return frames.length;
  return Math.min(frames.length, Math.floor(n));
}

function getPlaybackStartIndex() {
  const count = getPlaybackWindowCount();
  if (!frames.length) return 0;
  return Math.max(0, frames.length - count);
}

function schedulePlayback(delayMs, generation) {
  timer = setTimeout(async () => {
    timer = null;
    if (!isPlaying || generation !== playbackGeneration) return;
    try {
      await tickPlayback(generation);
    } catch (e) {
      console.warn("Playback tick failed:", e);
      setStatus(`Ошибка воспроизведения: ${e.message}`);
      isPlaying = false;
      playbackGeneration++;
      updatePlayButton();
    }
  }, delayMs);
}

async function tickPlayback(generation) {
  if (!isPlaying || generation !== playbackGeneration || !frames.length) return;
  const lastIdx = frames.length - 1;
  const startIdx = getPlaybackStartIndex();

  let nextIdx;
  if (currentFrame < startIdx || currentFrame > lastIdx) {
    nextIdx = startIdx;
  } else if (currentFrame >= lastIdx) {
    nextIdx = startIdx;
  } else {
    nextIdx = currentFrame + 1;
  }

  await renderFrame(nextIdx);
  if (!isPlaying || generation !== playbackGeneration) return;
  const delay = getPlaybackDelayMs(nextIdx, lastIdx);
  schedulePlayback(delay, generation);
}

function getPlaybackDelayMs(frameIdx, lastIdx) {
  const isLast = frameIdx === lastIdx;
  if (activeSource === SOURCE_SATELLITE) {
    return isLast
      ? SATELLITE_FRAME_LAST_HOLD_MS
      : SATELLITE_FRAME_FAST_INTERVAL_MS;
  }
  return isLast ? FRAME_LAST_HOLD_MS : FRAME_FAST_INTERVAL_MS;
}

function formatDistanceKm(meters) {
  const km = meters / 1000;
  if (km >= 100) return `${km.toFixed(0)} км`;
  if (km >= 10) return `${km.toFixed(1)} км`;
  return `${km.toFixed(2)} км`;
}

function clearMeasurementState() {
  measureStart = null;
  measureEnd = null;
  measurePointer = null;
  if (measureLayer?.getSource()) {
    measureLayer.getSource().clear();
  }
}

function renderMeasurement() {
  const source = measureLayer?.getSource?.();
  if (!source) return;
  source.clear();
  if (!measureStart) return;

  const pointStyle = new ol.style.Style({
    image: new ol.style.Circle({
      radius: 5,
      fill: new ol.style.Fill({ color: "rgba(9,105,218,0.95)" }),
      stroke: new ol.style.Stroke({
        color: "rgba(255,255,255,0.95)",
        width: 1.5,
      }),
    }),
  });

  const startFeature = new ol.Feature({
    geometry: new ol.geom.Point(measureStart),
  });
  startFeature.setStyle(pointStyle);
  source.addFeature(startFeature);

  const endCoord = measureEnd || measurePointer;
  if (!endCoord) return;

  const endFeature = new ol.Feature({
    geometry: new ol.geom.Point(endCoord),
  });
  endFeature.setStyle(pointStyle);
  source.addFeature(endFeature);

  const lineFeature = new ol.Feature({
    geometry: new ol.geom.LineString([measureStart, endCoord]),
  });
  lineFeature.setStyle(
    new ol.style.Style({
      stroke: new ol.style.Stroke({
        color: "rgba(9,105,218,0.95)",
        width: 2.5,
      }),
    }),
  );
  source.addFeature(lineFeature);

  const ll1 = ol.proj.transform(measureStart, "EPSG:3857", "EPSG:4326");
  const ll2 = ol.proj.transform(endCoord, "EPSG:3857", "EPSG:4326");
  const distM = ol.sphere.getDistance(ll1, ll2);
  const mid = [
    (measureStart[0] + endCoord[0]) / 2,
    (measureStart[1] + endCoord[1]) / 2,
  ];

  const labelFeature = new ol.Feature({
    geometry: new ol.geom.Point(mid),
  });
  labelFeature.setStyle(
    new ol.style.Style({
      text: new ol.style.Text({
        text: formatDistanceKm(distM),
        font: "bold 12px Segoe UI, sans-serif",
        fill: new ol.style.Fill({ color: "#1f2328" }),
        backgroundFill: new ol.style.Fill({ color: "rgba(255,255,255,0.95)" }),
        backgroundStroke: new ol.style.Stroke({
          color: "rgba(9,105,218,0.65)",
          width: 1,
        }),
        padding: [3, 6, 3, 6],
        offsetY: -10,
      }),
    }),
  );
  source.addFeature(labelFeature);
}

function setMeasureMode(enabled) {
  measureMode = Boolean(enabled);
  if (measureBtnEl) {
    measureBtnEl.classList.toggle("active", measureMode);
  }
  clearMeasurementState();
  if (measureMode) {
    setStatus("Режим измерения: кликните первую точку.");
  }
}

function createMap() {
  const projection = "EPSG:3857";
  baseLayer = new ol.layer.Tile({
    source: new ol.source.OSM({ wrapX: false }),
    opacity: 1,
    zIndex: 0,
  });

  frameLayer = new ol.layer.Image({
    className: "radar-layer",
    source: new ol.source.ImageStatic({
      url: TRANSPARENT_PIXEL,
      projection: "EPSG:4326",
      imageExtent: geoBounds,
      // Для радарной сетки используем nearest-neighbor, чтобы не замыливать пиксели.
      interpolate: false,
    }),
    opacity: HD_LAYER_OPACITY,
    zIndex: 5,
  });
  satelliteLayer = new ol.layer.Tile({
    source: null,
    opacity: 1,
    visible: false,
    zIndex: 4,
  });
  satelliteReferenceLayer = new ol.layer.Tile({
    source: new ol.source.XYZ({
      url: SATELLITE_REFERENCE_TILE_URL,
      crossOrigin: "anonymous",
      wrapX: true,
      interpolate: true,
    }),
    opacity: 0.95,
    visible: false,
    zIndex: 6,
  });

  map = new ol.Map({
    target: "map",
    layers: [baseLayer, satelliteLayer, frameLayer, satelliteReferenceLayer],
    view: new ol.View({
      projection,
      center: ol.proj.fromLonLat([
        (geoBounds[0] + geoBounds[2]) / 2,
        (geoBounds[1] + geoBounds[3]) / 2,
      ]),
      zoom: 4,
    }),
  });
  frameLayer.on("postrender", renderNowcastPixelGapOverlay);

  measureLayer = new ol.layer.Vector({
    source: new ol.source.Vector(),
    zIndex: 7,
  });
  map.addLayer(measureLayer);

  drawLayer = new ol.layer.Vector({
    source: new ol.source.Vector(),
    zIndex: 8,
    style: new ol.style.Style({
      stroke: new ol.style.Stroke({
        color: "rgba(31, 35, 40, 0.9)",
        width: 3,
      }),
    }),
  });
  map.addLayer(drawLayer);

  lightningLayer = new ol.layer.Vector({
    source: new ol.source.Vector(),
    visible: lightningOverlayEnabled,
    zIndex: 30,
  });
  map.addLayer(lightningLayer);
  if (lightningPopupEl) {
    lightningPopupOverlay = new ol.Overlay({
      element: lightningPopupEl,
      positioning: "bottom-center",
      offset: [0, -10],
      stopEvent: false,
    });
    map.addOverlay(lightningPopupOverlay);
  }

  const extent3857 = ol.proj.transformExtent(
    geoBounds,
    "EPSG:4326",
    "EPSG:3857",
  );
  map.getView().fit(extent3857, {
    size: map.getSize(),
    padding: [24, 24, 24, 24],
    maxZoom: 10,
  });

  map.on("pointermove", (event) => {
    if (measureMode && measureStart && !measureEnd) {
      measurePointer = event.coordinate.slice();
      renderMeasurement();
    }
    if (drawMode && drawCurrentStroke) {
      drawCurrentStroke.push(event.coordinate);
      drawCurrentStrokeFeature.getGeometry().setCoordinates(drawCurrentStroke);
    }
  });
  map.getView().on("change:resolution", () => {
    if (!frames.length) return;
    if (activeSource === SOURCE_HD || activeSource === SOURCE_SATELLITE) return;
    renderFrame(currentFrame).catch((e) => {
      console.warn("Nowcast rerender on zoom failed:", e);
    });
  });
  map.on("moveend", () => {
    inspectPrecipAtCrosshairCenter().catch(() => {});
    updateLightningVisibilityForFrame(true);
  });

  map.on("pointerdown", (event) => {
    if (!drawMode) return;
    drawCurrentStroke = [event.coordinate.slice()];
    const line = new ol.geom.LineString(drawCurrentStroke);
    drawCurrentStrokeFeature = new ol.Feature({ geometry: line });
    drawLayer.getSource().addFeature(drawCurrentStrokeFeature);
  });

  map.on("pointerup", () => {
    if (drawMode) drawCurrentStroke = null;
  });

  map.on("singleclick", (event) => {
    if (drawMode) return;
    if (handleLightningFeatureClick(event)) return;
    if (!measureMode) {
      hideLightningPopup();
      return;
    }
    if (!measureStart) {
      measureStart = event.coordinate.slice();
      measureEnd = null;
      measurePointer = event.coordinate.slice();
      renderMeasurement();
      setStatus("Режим измерения: выберите вторую точку.");
      return;
    }
    if (!measureEnd) {
      measureEnd = event.coordinate.slice();
      measurePointer = null;
      renderMeasurement();
      setStatus(
        "Измерение зафиксировано. Можно снова кликнуть для нового замера.",
      );
      return;
    }
    // Новый замер третьим кликом
    measureStart = event.coordinate.slice();
    measureEnd = null;
    measurePointer = event.coordinate.slice();
    renderMeasurement();
    setStatus("Новый замер: выберите вторую точку.");
  });
}

function renderNowcastPixelGapOverlay(event) {
  if (!NOWCAST_PIXEL_GAP_OVERLAY) return;
  if (!map || activeSource === SOURCE_SATELLITE) return;
  const frame = frames[currentFrame];
  if (
    !frame ||
    !Array.isArray(frame.imageExtent) ||
    frame.imageExtent.length !== 4
  )
    return;
  const width = Number(frame.imageWidth || radarImageSize?.[0] || 0);
  const height = Number(frame.imageHeight || radarImageSize?.[1] || 0);
  if (!(width > 1 && height > 1)) return;
  const ctx = event?.context;
  if (!ctx) return;

  const [west, south, east, north] = frame.imageExtent;
  const topLeft = map.getPixelFromCoordinate([west, north]);
  const topRight = map.getPixelFromCoordinate([east, north]);
  const bottomLeft = map.getPixelFromCoordinate([west, south]);
  if (!topLeft || !topRight || !bottomLeft) return;

  const renderWidthCss = Math.abs(topRight[0] - topLeft[0]);
  const renderHeightCss = Math.abs(bottomLeft[1] - topLeft[1]);
  if (!(renderWidthCss > 0 && renderHeightCss > 0)) return;

  const pixelStepXCss = renderWidthCss / width;
  const pixelStepYCss = renderHeightCss / height;
  const minStepCss = Math.min(pixelStepXCss, pixelStepYCss);
  if (
    !Number.isFinite(minStepCss) ||
    minStepCss < NOWCAST_PIXEL_GAP_MIN_SCREEN_PX
  )
    return;

  const pixelRatio =
    event?.frameState?.pixelRatio || window.devicePixelRatio || 1;
  const gapWidthCss = Math.max(
    0.35,
    Math.min(NOWCAST_PIXEL_GAP_MAX_SCREEN_PX, minStepCss * 0.16),
  );
  const alpha = Math.max(0.08, Math.min(0.24, 0.06 + minStepCss * 0.02));

  const leftCss = Math.min(topLeft[0], topRight[0]);
  const rightCss = Math.max(topLeft[0], topRight[0]);
  const topCss = Math.min(topLeft[1], bottomLeft[1]);
  const bottomCss = Math.max(topLeft[1], bottomLeft[1]);
  const stepX = pixelStepXCss * pixelRatio;
  const stepY = pixelStepYCss * pixelRatio;
  const left = leftCss * pixelRatio;
  const right = rightCss * pixelRatio;
  const top = topCss * pixelRatio;
  const bottom = bottomCss * pixelRatio;
  const gapWidth = gapWidthCss * pixelRatio;

  ctx.save();
  ctx.strokeStyle = `rgba(255,255,255,${alpha.toFixed(3)})`;
  ctx.lineWidth = gapWidth;
  ctx.beginPath();
  for (let x = left + stepX; x < right; x += stepX) {
    ctx.moveTo(x, top);
    ctx.lineTo(x, bottom);
  }
  for (let y = top + stepY; y < bottom; y += stepY) {
    ctx.moveTo(left, y);
    ctx.lineTo(right, y);
  }
  ctx.stroke();
  ctx.restore();
}

function normalizeFrames(payload) {
  const src = payload && Array.isArray(payload.frames) ? payload.frames : [];
  return src
    .map((f) => ({
      url: f.url || f.path || "",
      timestamp: new Date(f.time || f.timestamp || Date.now()),
      projection: "EPSG:4326",
      imageExtent: null,
    }))
    .filter((f) => f.url)
    .sort((a, b) => a.timestamp - b.timestamp);
}

function normalizeRadars(payload) {
  const src = payload && Array.isArray(payload.radars) ? payload.radars : [];
  return src
    .map((r) => ({
      lon: Number(r.lon),
      lat: Number(r.lat),
      radiusKm: Number(r.radiusKm),
    }))
    .filter(
      (r) =>
        Number.isFinite(r.lon) &&
        Number.isFinite(r.lat) &&
        Number.isFinite(r.radiusKm) &&
        r.radiusKm > 0,
    );
}

function upsertRadarLayer() {
  if (!map) return;
  if (radarLayer) {
    map.removeLayer(radarLayer);
    radarLayer = null;
  }
  if (!radars.length) return;

  const features = [];
  for (const r of radars) {
    const center3857 = ol.proj.fromLonLat([r.lon, r.lat]);
    const radiusM = r.radiusKm * 1000;
    features.push(
      new ol.Feature({
        geometry: new ol.geom.Circle(center3857, radiusM),
      }),
    );
  }

  const source = new ol.source.Vector({ features });
  radarLayer = new ol.layer.Vector({
    source,
    zIndex: 2,
    style: () =>
      new ol.style.Style({
        stroke: new ol.style.Stroke({
          color: "rgba(128, 128, 128, 0.6)",
          width: 0.5,
        }),
        fill: new ol.style.Fill({
          color: "rgba(128, 128, 128, 0.12)",
        }),
      }),
  });
  map.addLayer(radarLayer);
}

function getLightningViewBBox4326() {
  if (!map) return null;
  const size = map.getSize?.();
  if (!Array.isArray(size) || size.length < 2) return null;
  const extent3857 = map.getView()?.calculateExtent?.(size);
  if (!Array.isArray(extent3857) || extent3857.length < 4) return null;
  const extent4326 = ol.proj.transformExtent(
    extent3857,
    "EPSG:3857",
    "EPSG:4326",
  );
  if (!Array.isArray(extent4326) || extent4326.length < 4) return null;
  return extent4326;
}

function hideLightningPopup() {
  lightningPopupOverlay?.setPosition?.(undefined);
  lightningPopupEl?.classList?.add("hidden");
}

function showLightningPopup(coordinate, strikeTime, ageMinutes) {
  if (!lightningPopupEl || !lightningPopupOverlay || !Array.isArray(coordinate))
    return;
  const parsed = Date.parse(strikeTime || "");
  const localText = Number.isFinite(parsed)
    ? new Date(parsed).toLocaleString("ru-RU")
    : String(strikeTime || "неизвестно");
  const ageText = Number.isFinite(ageMinutes)
    ? ` (${Math.round(ageMinutes)} мин назад)`
    : "";
  lightningPopupEl.textContent = `⚡ ${localText}${ageText}`;
  lightningPopupEl.classList.remove("hidden");
  lightningPopupOverlay.setPosition(coordinate);
}

function handleLightningFeatureClick(event) {
  if (!map || !lightningOverlayEnabled || !lightningLayer?.getVisible?.())
    return false;
  let picked = null;
  map.forEachFeatureAtPixel(
    event.pixel,
    (feature, layer) => {
      if (layer !== lightningLayer) return undefined;
      picked = feature;
      return feature;
    },
    { hitTolerance: 8 },
  );
  if (!picked) return false;
  const coordinate = picked.getGeometry?.()?.getCoordinates?.();
  showLightningPopup(
    coordinate,
    picked.get("strikeTime"),
    picked.get("ageMinutes"),
  );
  return true;
}

function clearLightningLayer() {
  lightningLayer?.getSource?.()?.clear();
  hideLightningPopup();
}

function shouldShowLightningOnCurrentFrame() {
  if (!lightningOverlayEnabled) return false;
  if (isPlaying) return false;
  if (!Array.isArray(frames) || !frames.length) return false;
  return currentFrame === frames.length - 1;
}

function updateLightningVisibilityForFrame(refreshIfVisible = false) {
  const visible = shouldShowLightningOnCurrentFrame();
  lightningLayer?.setVisible(visible);
  if (!visible) {
    hideLightningPopup();
    return;
  }
  if (refreshIfVisible) {
    refreshLightningOverlay().catch(() => {});
  }
}

function getLightningPointStyleByAge(ageMinutes) {
  const clamped = Math.max(0, Math.min(60, Number(ageMinutes) || 0));
  const alpha = Math.max(0.28, 1 - clamped / 70);
  const radius = clamped <= 5 ? 6.2 : clamped <= 15 ? 5.2 : 4.2;
  const fill =
    clamped <= 10
      ? `rgba(255, 237, 64, ${alpha.toFixed(3)})`
      : `rgba(255, 173, 42, ${alpha.toFixed(3)})`;
  return new ol.style.Style({
    image: new ol.style.Circle({
      radius,
      fill: new ol.style.Fill({ color: fill }),
      stroke: new ol.style.Stroke({
        color: `rgba(20,20,20,${Math.min(0.95, alpha + 0.35).toFixed(3)})`,
        width: 1.2,
      }),
    }),
  });
}

function renderLightningStrikes(strikes) {
  const source = lightningLayer?.getSource?.();
  if (!source) return;
  source.clear();
  if (!Array.isArray(strikes) || !strikes.length) return;
  const nowMs = Date.now();
  const features = [];
  for (const s of strikes) {
    const lat = Number(s.lat);
    const lon = Number(s.lon);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
    const tMs = Date.parse(s.time || "");
    const ageMin = Number.isFinite(tMs)
      ? Math.max(0, (nowMs - tMs) / 60000)
      : 999;
    const feature = new ol.Feature({
      geometry: new ol.geom.Point(ol.proj.fromLonLat([lon, lat])),
    });
    feature.set("ageMinutes", ageMin);
    feature.set("strikeTime", s.time || "");
    feature.setStyle(getLightningPointStyleByAge(ageMin));
    features.push(feature);
  }
  if (features.length) source.addFeatures(features);
}

async function refreshLightningOverlay() {
  if (!shouldShowLightningOnCurrentFrame()) {
    hideLightningPopup();
    return;
  }
  const reqToken = ++lightningRequestToken;
  const bbox = getLightningViewBBox4326();
  const params = new URLSearchParams();
  params.set("minutes", "60");
  params.set("limit", "8000");
  params.set("stations", "GLD2NE,GLD3N,GLD4S,MTG1");
  const withBBox = new URLSearchParams(params);
  if (bbox) {
    withBBox.set("west", String(bbox[0]));
    withBBox.set("south", String(bbox[1]));
    withBBox.set("east", String(bbox[2]));
    withBBox.set("north", String(bbox[3]));
  }
  try {
    const resp = await fetch(
      `/api/lightning/gld360/latest?${withBBox.toString()}`,
      {
        cache: "no-store",
      },
    );
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const payload = await resp.json();
    if (reqToken !== lightningRequestToken) return;
    const first = Array.isArray(payload?.strikes) ? payload.strikes : [];
    if (first.length > 0) {
      renderLightningStrikes(first);
      return;
    }
    // Fallback: if local bbox has no strikes, fetch wider area.
    const respWide = await fetch(
      `/api/lightning/gld360/latest?${params.toString()}`,
      {
        cache: "no-store",
      },
    );
    if (!respWide.ok) throw new Error(`HTTP ${respWide.status}`);
    const payloadWide = await respWide.json();
    if (reqToken !== lightningRequestToken) return;
    renderLightningStrikes(payloadWide?.strikes || []);
  } catch (e) {
    if (reqToken !== lightningRequestToken) return;
    console.warn("Lightning overlay refresh failed:", e);
    clearLightningLayer();
  }
}

function setLightningOverlayEnabled(enabled) {
  lightningOverlayEnabled = Boolean(enabled);
  lightningToggleBtnEl?.classList.toggle("active", lightningOverlayEnabled);
  if (!lightningOverlayEnabled) {
    clearLightningLayer();
    return;
  }
  updateLightningVisibilityForFrame(true);
}

function setFrameExtentAndUrl(extent, url, projection, interpolate = false) {
  frameLayer.setSource(
    new ol.source.ImageStatic({
      url: url || TRANSPARENT_PIXEL,
      projection: projection || "EPSG:4326",
      imageExtent: extent,
      // Для спутника включаем сглаживание, для радарной сетки оставляем пиксельный nearest.
      interpolate,
    }),
  );
}

function getDbzLabelValue(label) {
  const match = String(label || "").match(/-?\d+(?:\.\d+)?/);
  if (!match) return null;
  const value = Number(match[0]);
  return Number.isFinite(value) ? value : null;
}

function classifyDbzGreenCluster(r, g, b, items) {
  const lowBand = [];
  const highBand = [];
  for (const item of items) {
    const v = getDbzLabelValue(item.label);
    if (v === 0 || v === 5) {
      lowBand.push(item);
    } else if (v === 50 || v === 55) {
      highBand.push(item);
    }
  }
  if (!lowBand.length || !highBand.length) return null;

  const nearest = (group) => {
    let best = group[0];
    let bestDist = Infinity;
    for (const p of group) {
      const dr = r - p.rgb[0];
      const dg = g - p.rgb[1];
      const db = b - p.rgb[2];
      const dist = dr * dr + dg * dg + db * db;
      if (dist < bestDist) {
        bestDist = dist;
        best = p;
      }
    }
    return { item: best, distSq: bestDist };
  };

  const low = nearest(lowBand);
  const high = nearest(highBand);
  // Для зелёного кластера в nowcast 0/5 dBZ заметно светлее, чем 50/55 dBZ.
  const luma = 0.2126 * r + 0.7152 * g + 0.0722 * b;
  if (Math.abs(low.distSq - high.distSq) <= 26 * 26) {
    return luma >= 186 ? low.item.label : high.item.label;
  }
  return low.distSq <= high.distSq ? low.item.label : high.item.label;
}

function getLegendLabelByRgb(sourceKey, r, g, b, alpha = 255) {
  const cfg = getLegendConfigForSource(sourceKey);
  const items = cfg?.items || [];
  if (!items.length) return null;
  if (alpha === 0) return items[0].label;
  if (
    (sourceKey === "bufr_height" ||
      sourceKey === "bufr_dbz1" ||
      sourceKey === "bufr_precip") &&
    alpha < 24
  ) {
    return items[0].label;
  }

  let best = items[0];
  let bestDist = Infinity;
  let second = items[0];
  let secondDist = Infinity;
  for (const p of items) {
    const dr = r - p.rgb[0];
    const dg = g - p.rgb[1];
    const db = b - p.rgb[2];
    const dist = Math.sqrt(dr * dr + dg * dg + db * db);
    if (dist < bestDist) {
      secondDist = bestDist;
      second = best;
      bestDist = dist;
      best = p;
    } else if (dist < secondDist) {
      secondDist = dist;
      second = p;
    }
  }

  if (sourceKey === "bufr_dbz1") {
    const bestVal = getDbzLabelValue(best.label);
    const secondVal = getDbzLabelValue(second.label);
    const isGreenAmbiguous = (v) => v === 0 || v === 5 || v === 50 || v === 55;
    if (isGreenAmbiguous(bestVal) || isGreenAmbiguous(secondVal)) {
      const fixed = classifyDbzGreenCluster(r, g, b, items);
      if (fixed) return fixed;
    }
  }

  if (bestDist <= (cfg?.maxDist || PHENOMENA_MATCH_MAX_DIST)) return best.label;
  if (cfg?.forceNearest) return best.label;

  // Для "Опасных явлений" оставляем чуть более мягкий fallback на синие ливневые тона.
  if (sourceKey === SOURCE_HD || sourceKey === "bufr_phenomena") {
    const looksBlueShower = b > g && b > r && r < 130;
    if (looksBlueShower) {
      const showerCandidates = PHENOMENA_LEGEND.filter((p) =>
        p.label.startsWith("ливень"),
      );
      let showerBest = null;
      let showerBestDist = Infinity;
      for (const p of showerCandidates) {
        const dr = r - p.rgb[0];
        const dg = g - p.rgb[1];
        const db = b - p.rgb[2];
        const dist = Math.sqrt(dr * dr + dg * dg + db * db);
        if (dist < showerBestDist) {
          showerBestDist = dist;
          showerBest = p;
        }
      }
      if (showerBest && showerBestDist <= SHOWER_MATCH_MAX_DIST) {
        return showerBest.label;
      }
    }
  }

  return null;
}

async function getFramePixels(url) {
  if (framePixelCache.has(url)) return framePixelCache.get(url);
  const img = await new Promise((resolve, reject) => {
    const image = new Image();
    image.onload = () => resolve(image);
    image.onerror = () => reject(new Error("Image load failed"));
    image.src = url;
  });
  const canvas = document.createElement("canvas");
  canvas.width = img.width;
  canvas.height = img.height;
  const ctx = canvas.getContext("2d", { willReadFrequently: true });
  ctx.drawImage(img, 0, 0);
  const imageData = ctx.getImageData(0, 0, img.width, img.height);
  const payload = {
    width: img.width,
    height: img.height,
    data: imageData.data,
  };
  framePixelCache.set(url, payload);
  if (framePixelCache.size > 8) {
    const first = framePixelCache.keys().next().value;
    framePixelCache.delete(first);
  }
  return payload;
}

async function getFrameImageSizeCached(url) {
  if (!url) return { width: 0, height: 0 };
  if (frameImageSizeCache.has(url)) return frameImageSizeCache.get(url);
  const size = await new Promise((resolve, reject) => {
    const image = new Image();
    image.onload = () =>
      resolve({
        width: Number(image.naturalWidth || image.width || 0),
        height: Number(image.naturalHeight || image.height || 0),
      });
    image.onerror = () => reject(new Error("Image size load failed"));
    image.src = url;
  });
  frameImageSizeCache.set(url, size);
  if (frameImageSizeCache.size > 64) {
    const first = frameImageSizeCache.keys().next().value;
    frameImageSizeCache.delete(first);
  }
  return size;
}

async function getStyledFrameUrl(url) {
  if (!RADAR_PIXEL_CUT_STYLE) return url;
  if (frameStyledUrlCache.has(url)) return frameStyledUrlCache.get(url);

  const px = await getFramePixels(url);
  const canvas = document.createElement("canvas");
  canvas.width = px.width;
  canvas.height = px.height;
  const ctx = canvas.getContext("2d");
  const out = new ImageData(px.width, px.height);
  out.data.set(px.data);

  for (let y = 0; y < px.height; y++) {
    for (let x = 0; x < px.width; x++) {
      const i = (y * px.width + x) * 4;
      const a = out.data[i + 3];
      if (a === 0) continue;
      if (DISPLAY_SOFT_PALETTE_SNAP) {
        const r = out.data[i];
        const g = out.data[i + 1];
        const b = out.data[i + 2];
        const { best, bestDist, secondDist } = getNearestLegendColorInfo(
          r,
          g,
          b,
        );
        if (
          bestDist <= DISPLAY_SNAP_MAX_DIST_SQ &&
          secondDist - bestDist >= DISPLAY_SNAP_MIN_GAP_SQ
        ) {
          out.data[i] = best.rgb[0];
          out.data[i + 1] = best.rgb[1];
          out.data[i + 2] = best.rgb[2];
        }
      }
      const cutX = x % RADAR_PIXEL_CUT_STEP === RADAR_PIXEL_CUT_STEP - 1;
      const cutY = y % RADAR_PIXEL_CUT_STEP === RADAR_PIXEL_CUT_STEP - 1;
      if (cutX || cutY) {
        out.data[i + 3] = Math.max(8, Math.round(a * RADAR_PIXEL_CUT_ALPHA));
      }
    }
  }

  ctx.putImageData(out, 0, 0);
  const styledUrl = canvas.toDataURL("image/png");
  frameStyledUrlCache.set(url, styledUrl);
  if (frameStyledUrlCache.size > 12) {
    const first = frameStyledUrlCache.keys().next().value;
    frameStyledUrlCache.delete(first);
  }
  return styledUrl;
}

async function inspectPrecipAtCrosshairCenter() {
  if (!crosshairMode) return;
  if (activeSource === SOURCE_SATELLITE) return;
  if (!map || !frames.length) return;
  const frame = frames[currentFrame];
  if (!frame?.url) return;
  const probeToken = ++centerProbeToken;
  try {
    const center3857 = map.getView()?.getCenter?.();
    if (!Array.isArray(center3857) || center3857.length < 2) return;
    const px = await getFramePixels(frame.url);
    if (probeToken !== centerProbeToken) return;

    const projection = frame?.projection || "EPSG:4326";
    const extent =
      Array.isArray(frame?.imageExtent) && frame.imageExtent.length === 4
        ? frame.imageExtent
        : projection === "EPSG:3857"
          ? geoBounds3857
          : geoBounds;
    if (!Array.isArray(extent) || extent.length < 4) return;

    let coordX = center3857[0];
    let coordY = center3857[1];
    if (projection !== "EPSG:3857") {
      const ll = ol.proj.transform(center3857, "EPSG:3857", "EPSG:4326");
      coordX = ll[0];
      coordY = ll[1];
    }
    const [west, south, east, north] = extent;
    if (!(east > west && north > south)) return;

    const x = Math.round(((coordX - west) / (east - west)) * (px.width - 1));
    const y = Math.round(
      ((north - coordY) / (north - south)) * (px.height - 1),
    );
    if (x < 0 || x >= px.width || y < 0 || y >= px.height) {
      setPrecipInfo("В перекрестии: вне зоны радарного кадра");
      return;
    }

    const i = (y * px.width + x) * 4;
    const r = px.data[i];
    const g = px.data[i + 1];
    const b = px.data[i + 2];
    const a = px.data[i + 3];
    const label = getLegendLabelByRgb(activeSource, r, g, b, a);
    if (!label) {
      setPrecipInfo(`В перекрестии: неопределено (RGB ${r},${g},${b})`);
      return;
    }
    setPrecipInfo(`В перекрестии: ${label}`);
  } catch {
    if (probeToken === centerProbeToken) {
      setPrecipInfo("В перекрестии: не удалось определить тип");
    }
  }
}

function setDrawMode(enabled) {
  drawMode = Boolean(enabled);
  if (drawBtnEl) drawBtnEl.classList.toggle("active", drawMode);
  drawCurrentStroke = null;
  if (!drawMode && drawLayer?.getSource()) {
    drawLayer.getSource().clear();
  }
  // В режиме рисования отключаем перетаскивание карты, чтобы двигался курсор, а не карта
  if (map) {
    map.getInteractions().forEach(function (interaction) {
      if (interaction instanceof ol.interaction.DragPan) {
        interaction.setActive(!drawMode);
      }
    });
  }
  if (drawMode)
    setStatus("Режим рисования: ведите курсор с зажатой кнопкой мыши.");
}

function syncMapViewportAfterResize() {
  if (!map) return;
  if (viewportSyncTimer) {
    clearTimeout(viewportSyncTimer);
  }
  // На мобильных после поворота размер viewport меняется каскадно:
  // делаем отложенную синхронизацию, чтобы слои и пиксельная сетка не смещались.
  viewportSyncTimer = setTimeout(() => {
    viewportSyncTimer = null;
    map.updateSize();
    inspectPrecipAtCrosshairCenter().catch(() => {});
    if (!frames.length) return;
    renderFrame(currentFrame).catch((e) => {
      console.warn("Frame rerender after viewport resize failed:", e);
    });
  }, 140);
}

async function loadNowcastMeta(preferredLayers = "") {
  const response = await fetch("/api/nowcast/meta", { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Nowcast meta HTTP ${response.status}`);
  }
  nowcastMeta = await response.json();
  return nowcastMeta;
}

function rebuildSourceOptions() {
  if (!sourceLayerSelectEl && !mobileSourceLayerSelectEl) return;
  const prev =
    sourceLayerSelectEl?.value || mobileSourceLayerSelectEl?.value || SOURCE_HD;
  const options = [
    { value: SOURCE_HD, label: "Опасные явления HD" },
    { value: SOURCE_SATELLITE, label: SATELLITE_LAYER_LABEL },
  ];

  const availableNames = new Set(
    (Array.isArray(nowcastMeta?.layers) ? nowcastMeta.layers : [])
      .map((l) => String(l?.name || "").trim())
      .filter(Boolean),
  );

  for (const name of NOWCAST_LAYER_ORDER) {
    if (!availableNames.has(name)) continue;
    options.push({
      value: name,
      label: NOWCAST_LAYER_LABELS[name] || name,
    });
  }

  const fillSelect = (selectEl) => {
    if (!selectEl) return;
    selectEl.innerHTML = "";
    for (const item of options) {
      const opt = document.createElement("option");
      opt.value = item.value;
      opt.textContent = item.label;
      selectEl.appendChild(opt);
    }
  };
  fillSelect(sourceLayerSelectEl);
  fillSelect(mobileSourceLayerSelectEl);

  const selected = options.some((item) => item.value === prev)
    ? prev
    : SOURCE_HD;
  setSourceSelectValue(selected);
}

async function loadNowcastFrames(layerName) {
  const layer = String(layerName || "").trim();
  if (!layer || layer === SOURCE_HD) return [];
  if (nowcastFramesByLayer.has(layer)) return nowcastFramesByLayer.get(layer);

  const [width0, height0] = radarImageSize;
  const width = Number.isFinite(width0) && width0 > 0 ? width0 : 1024;
  const height = Number.isFinite(height0) && height0 > 0 ? height0 : 768;
  const [west, south, east, north] = geoBounds;

  const playbackRaw = playbackFrameCountEl?.value || "18";
  let limit = 18;
  if (playbackRaw === "all") limit = 36;
  else {
    const n = Number(playbackRaw);
    if (Number.isFinite(n) && n > 0) {
      limit = Math.min(NOWCAST_PREFETCH_MAX_FRAMES, Math.max(6, Math.floor(n)));
    }
  }

  const params = new URLSearchParams({
    layer,
    version: NOWCAST_WMS_VERSION,
    crs: "EPSG:3857",
    width: String(width),
    height: String(height),
    west: String(west),
    south: String(south),
    east: String(east),
    north: String(north),
    limit: String(limit),
  });
  setStatus(`Загрузка (${layer})...`);
  const response = await fetch(`/api/nowcast/frames?${params.toString()}`, {
    cache: "no-store",
  });
  if (!response.ok) {
    throw new Error(`Radar frames HTTP ${response.status}`);
  }
  const payload = await response.json();
  const sourceFrames = Array.isArray(payload?.frames) ? payload.frames : [];
  const normalizedRaw = sourceFrames
    .map((f) => ({
      url: f.url || "",
      sourceUrl: f.url || "",
      timestamp: new Date(f.time || f.timestamp || Date.now()),
      projection: f.projection || "EPSG:3857",
      imageExtent: Array.isArray(f.imageExtent) ? f.imageExtent : geoBounds3857,
      imageWidth: width,
      imageHeight: height,
    }))
    .filter(
      (f) =>
        f.url && Array.isArray(f.imageExtent) && f.imageExtent.length === 4,
    )
    .sort((a, b) => a.timestamp - b.timestamp);
  nowcastFramesByLayer.set(layer, normalizedRaw);
  warmNowcastFramesInBackground(layer, normalizedRaw);
  return normalizedRaw;
}

async function loadSatelliteFrames() {
  const cacheMode = "rgb";
  const framesCacheKey = `auto|${SATELLITE_FRAME_LIMIT}|${SATELLITE_CADENCE_MIN}|${cacheMode}`;
  if (satelliteFramesByKey.has(framesCacheKey)) {
    return satelliteFramesByKey.get(framesCacheKey);
  }
  const [west, south, east, north] = geoBounds;
  const [width0, height0] = radarImageSize;
  const width = Number.isFinite(width0) && width0 > 0 ? width0 : 1200;
  const height = Number.isFinite(height0) && height0 > 0 ? height0 : 1200;
  const params = new URLSearchParams({
    layer: "auto",
    limit: String(SATELLITE_FRAME_LIMIT),
    cadenceMin: String(SATELLITE_CADENCE_MIN),
    west: String(west),
    south: String(south),
    east: String(east),
    north: String(north),
    width: String(width),
    height: String(height),
  });
  setStatus("Загрузка спутниковых кадров...");
  const response = await fetch(`/api/satellite/frames?${params.toString()}`, {
    cache: "no-store",
  });
  if (!response.ok) {
    throw new Error(`Satellite frames HTTP ${response.status}`);
  }
  const payload = await response.json();
  satelliteResolvedLayer = String(payload?.layer || "").trim();
  const sourceFrames = Array.isArray(payload?.frames) ? payload.frames : [];
  const normalizedRaw = sourceFrames
    .map((f) => ({
      url: f.url || "",
      sourceUrl: f.url || "",
      timestamp: new Date(f.time || f.timestamp || Date.now()),
      projection: f.projection || "EPSG:4326",
      imageExtent: Array.isArray(f.imageExtent) ? f.imageExtent : geoBounds,
      imageWidth: width,
      imageHeight: height,
    }))
    .filter(
      (f) =>
        f.url && Array.isArray(f.imageExtent) && f.imageExtent.length === 4,
    )
    .sort((a, b) => a.timestamp - b.timestamp);
  satelliteFramesByKey.set(framesCacheKey, normalizedRaw);
  warmSatelliteFramesInBackground(framesCacheKey, normalizedRaw);
  return normalizedRaw;
}

function warmNowcastFramesInBackground(layer, framesList) {
  const target = Array.isArray(framesList) ? framesList : [];
  if (!target.length) return;
  (async () => {
    for (let i = 0; i < target.length; i++) {
      const frame = target[i];
      if (!frame?.sourceUrl || nowcastBlobUrlCache.has(frame.sourceUrl))
        continue;
      try {
        const localUrl = await getNowcastBlobUrlCached(frame.sourceUrl);
        frame.url = localUrl;
      } catch (e) {
        console.warn(`Nowcast warmup failed (${layer} #${i + 1}):`, e);
      }
    }
  })();
}

function warmSatelliteFramesInBackground(cacheKey, framesList) {
  const target = Array.isArray(framesList) ? framesList : [];
  if (!target.length) return;
  (async () => {
    for (let i = 0; i < target.length; i++) {
      const frame = target[i];
      if (!frame?.sourceUrl) continue;
      try {
        const localUrl = await getSatelliteBlobUrlCached(frame.sourceUrl);
        frame.url = localUrl;
      } catch (e) {
        console.warn(`Satellite warmup failed (${cacheKey} #${i + 1}):`, e);
      }
    }
  })();
}

async function getNowcastBlobUrlCached(sourceUrl) {
  if (nowcastBlobUrlCache.has(sourceUrl)) {
    return nowcastBlobUrlCache.get(sourceUrl);
  }
  const response = await fetch(sourceUrl, { cache: "force-cache" });
  if (!response.ok) {
    throw new Error(`Frame HTTP ${response.status}`);
  }
  const blob = await response.blob();
  const cleanedBlob = await removeNowcastRadarBackground(blob);
  const localUrl = URL.createObjectURL(cleanedBlob);
  nowcastBlobUrlCache.set(sourceUrl, localUrl);
  nowcastBlobUrlOrder.push(sourceUrl);

  // Ограничиваем память: удаляем самые старые blob-URL.
  const hardLimit = 220;
  while (nowcastBlobUrlOrder.length > hardLimit) {
    const key = nowcastBlobUrlOrder.shift();
    if (!key) break;
    const url = nowcastBlobUrlCache.get(key);
    if (url) {
      URL.revokeObjectURL(url);
      nowcastBlobUrlCache.delete(key);
    }
  }
  return localUrl;
}

async function getSatelliteBlobUrlCached(sourceUrl) {
  const styleCacheKey = getSatelliteStyleCacheKey(sourceUrl);
  if (satelliteBlobUrlCache.has(styleCacheKey)) {
    return satelliteBlobUrlCache.get(styleCacheKey);
  }
  const response = await fetch(sourceUrl, { cache: "force-cache" });
  if (!response.ok) {
    throw new Error(`Satellite frame HTTP ${response.status}`);
  }
  const blob = await response.blob();
  const styledBlob = await styleSatelliteTileCTA(blob);
  const localUrl = URL.createObjectURL(styledBlob);
  satelliteBlobUrlCache.set(styleCacheKey, localUrl);
  satelliteBlobUrlOrder.push(styleCacheKey);

  const hardLimit = 180;
  while (satelliteBlobUrlOrder.length > hardLimit) {
    const key = satelliteBlobUrlOrder.shift();
    if (!key) break;
    const url = satelliteBlobUrlCache.get(key);
    if (url) {
      URL.revokeObjectURL(url);
      satelliteBlobUrlCache.delete(key);
    }
  }
  return localUrl;
}

async function removeNowcastRadarBackground(blob) {
  const canvas = document.createElement("canvas");
  const ctx = canvas.getContext("2d", { willReadFrequently: true });
  if (typeof createImageBitmap === "function") {
    const bitmap = await createImageBitmap(blob);
    canvas.width = bitmap.width;
    canvas.height = bitmap.height;
    ctx.drawImage(bitmap, 0, 0);
    bitmap.close();
  } else {
    const imageUrl = URL.createObjectURL(blob);
    const image = await new Promise((resolve, reject) => {
      const img = new Image();
      img.onload = () => resolve(img);
      img.onerror = () => reject(new Error("Nowcast image decode failed"));
      img.src = imageUrl;
    });
    URL.revokeObjectURL(imageUrl);
    canvas.width = image.width;
    canvas.height = image.height;
    ctx.drawImage(image, 0, 0);
  }

  const imageData = ctx.getImageData(0, 0, canvas.width, canvas.height);
  const data = imageData.data;
  const width = canvas.width;
  const height = canvas.height;
  const [tr, tg, tb] = NOWCAST_RADAR_BG_RGB;
  const maxDistSq = NOWCAST_RADAR_BG_TOLERANCE * NOWCAST_RADAR_BG_TOLERANCE;
  const [gr, gg, gb] = NOWCAST_GRAY_BG_RGB;
  const maxGrayDistSq = NOWCAST_GRAY_BG_TOLERANCE * NOWCAST_GRAY_BG_TOLERANCE;

  for (let i = 0; i < data.length; i += 4) {
    const a = data[i + 3];
    if (a === 0) continue;
    const r = data[i];
    const g = data[i + 1];
    const b = data[i + 2];
    const dr = r - tr;
    const dg = g - tg;
    const db = b - tb;
    const distSq = dr * dr + dg * dg + db * db;

    const drGray = r - gr;
    const dgGray = g - gg;
    const dbGray = b - gb;
    const grayDistSq = drGray * drGray + dgGray * dgGray + dbGray * dbGray;
    const maxC = Math.max(r, g, b);
    const minC = Math.min(r, g, b);
    const isNeutralGray = maxC - minC <= 14;
    const avg = (r + g + b) / 3;
    const isLikelyTileBg =
      a >= 120 &&
      a <= 220 &&
      isNeutralGray &&
      avg >= 145 &&
      (grayDistSq <= maxGrayDistSq || distSq <= maxDistSq);

    if (distSq <= maxDistSq || isLikelyTileBg) {
      data[i + 3] = 0;
      continue;
    }

    // Чуть уменьшаем общую прозрачность nowcast-цветов.
    data[i + 3] = Math.max(1, Math.round(a * NOWCAST_COLOR_ALPHA));
  }

  if (NOWCAST_EDGE_CUT_STYLE) {
    // Аккуратный "разрез": подчеркиваем только реальные границы блоков/контраста.
    const src = new Uint8ClampedArray(data);
    const hasEdgeNeighbor = (x, y, r, g, b) => {
      const neighbors = [
        [x - 1, y],
        [x + 1, y],
        [x, y - 1],
        [x, y + 1],
      ];
      for (const [nx, ny] of neighbors) {
        if (nx < 0 || nx >= width || ny < 0 || ny >= height) continue;
        const ni = (ny * width + nx) * 4;
        const na = src[ni + 3];
        if (na === 0) return true;
        const dr = r - src[ni];
        const dg = g - src[ni + 1];
        const db = b - src[ni + 2];
        const dist = Math.sqrt(dr * dr + dg * dg + db * db);
        if (dist >= NOWCAST_EDGE_COLOR_DIFF) return true;
      }
      return false;
    };

    for (let y = 0; y < height; y++) {
      for (let x = 0; x < width; x++) {
        const i = (y * width + x) * 4;
        const a = src[i + 3];
        if (a === 0) continue;
        const r = src[i];
        const g = src[i + 1];
        const b = src[i + 2];
        if (!hasEdgeNeighbor(x, y, r, g, b)) continue;

        data[i + 3] = Math.max(8, Math.round(a * NOWCAST_EDGE_CUT_ALPHA));
        data[i] = Math.min(255, r + NOWCAST_EDGE_CUT_BRIGHTEN);
        data[i + 1] = Math.min(255, g + NOWCAST_EDGE_CUT_BRIGHTEN);
        data[i + 2] = Math.min(255, b + NOWCAST_EDGE_CUT_BRIGHTEN);
      }
    }
  }

  ctx.putImageData(imageData, 0, 0);
  return await new Promise((resolve, reject) => {
    canvas.toBlob(
      (out) => {
        if (!out) {
          reject(new Error("Frame background cleanup failed"));
          return;
        }
        resolve(out);
      },
      "image/png",
      1,
    );
  });
}

async function switchSource(layerOrHd) {
  const switchToken = ++sourceSwitchToken;
  const next = String(layerOrHd || SOURCE_HD);
  stopPlayback(false);

  if (next === SOURCE_SATELLITE) {
    const satelliteFrames = await loadSatelliteFrames();
    if (switchToken !== sourceSwitchToken) return;
    if (!satelliteFrames.length) {
      setStatus("Спутниковые кадры сейчас недоступны");
      setSourceSelectValue(activeSource || SOURCE_HD);
      return;
    }
    activeSource = SOURCE_SATELLITE;
    setSourceSelectValue(activeSource);
    renderActiveLegend();
    baseLayer?.setVisible(false);
    frameLayer?.setVisible(true);
    satelliteLayer?.setVisible(false);
    satelliteReferenceLayer?.setVisible(true);
    frameLayer?.setOpacity(1);
    updateSatelliteStylePanelVisibility();
    setCrosshairMode(crosshairMode);
    setPlaybackControlsEnabled(true);
    frames = satelliteFrames.slice();
    currentFrame = Math.max(0, frames.length - 1);
    timelineEl.max = String(Math.max(0, frames.length - 1));
    await renderFrame(currentFrame);
    if (switchToken !== sourceSwitchToken) return;
    if (frames.length > 1) startPlayback();
    const resolved = satelliteResolvedLayer
      ? ` [${satelliteResolvedLayer}]`
      : "";
    setStatus(
      `Слой: ${getSourceDisplayName(SOURCE_SATELLITE)}${resolved} (кадров: ${frames.length})`,
    );
    return;
  }

  if (next === SOURCE_HD) {
    activeSource = SOURCE_HD;
    setSourceSelectValue(activeSource);
    renderActiveLegend();
    baseLayer?.setVisible(true);
    frameLayer?.setVisible(true);
    satelliteLayer?.setVisible(false);
    satelliteReferenceLayer?.setVisible(false);
    frameLayer?.setOpacity(HD_LAYER_OPACITY);
    updateSatelliteStylePanelVisibility();
    setCrosshairMode(crosshairMode);
    setPlaybackControlsEnabled(true);
    frames = hdFrames.slice();
    timelineEl.max = String(Math.max(0, frames.length - 1));
    const startIdx = frames.length
      ? Math.min(currentFrame, frames.length - 1)
      : 0;
    await renderFrame(startIdx);
    if (switchToken !== sourceSwitchToken) return;
    if (frames.length > 1) startPlayback();
    setStatus(`Слой: ${getSourceDisplayName(SOURCE_HD)}`);
    return;
  }

  const nowcastFrames = await loadNowcastFrames(next);
  if (switchToken !== sourceSwitchToken) return;
  if (!nowcastFrames.length) {
    setStatus(`4x4 слой ${next}: кадры недоступны`);
    return;
  }
  activeSource = next;
  setSourceSelectValue(activeSource);
  renderActiveLegend();
  baseLayer?.setVisible(true);
  frameLayer?.setVisible(true);
  satelliteLayer?.setVisible(false);
  satelliteReferenceLayer?.setVisible(false);
  frameLayer?.setOpacity(NOWCAST_LAYER_OPACITY);
  updateSatelliteStylePanelVisibility();
  setCrosshairMode(crosshairMode);
  setPlaybackControlsEnabled(true);
  frames = nowcastFrames.slice();
  currentFrame = Math.max(0, frames.length - 1);
  timelineEl.max = String(Math.max(0, frames.length - 1));
  await renderFrame(currentFrame);
  if (switchToken !== sourceSwitchToken) return;
  if (frames.length > 1) startPlayback();
  setStatus(`Слой: ${getSourceDisplayName(next)} (кадров: ${frames.length})`);
}

async function renderFrame(index) {
  if (!frames.length) return;
  const renderToken = ++frameRenderToken;
  currentFrame = ((index % frames.length) + frames.length) % frames.length;
  const frame = frames[currentFrame];
  if (frame?.satelliteTile && frame?.satelliteTileTemplate) {
    const source = getSatelliteTileSource(frame.satelliteTileTemplate);
    if (renderToken !== frameRenderToken) return;
    satelliteLayer?.setSource(source);
    frameLayer?.setVisible(false);
    satelliteLayer?.setVisible(true);
    satelliteReferenceLayer?.setVisible(activeSource === SOURCE_SATELLITE);
    timelineEl.value = String(currentFrame);
    timeLabelEl.textContent = frame.timestamp.toLocaleString("ru-RU");
    const layerLabel = getSourceDisplayName(activeSource);
    setStatus(`кадр ${currentFrame + 1} / ${frames.length}`);
    updateLightningVisibilityForFrame(true);
    inspectPrecipAtCrosshairCenter().catch(() => {});
    return;
  }
  const projection = frame?.projection || "EPSG:4326";
  const extent =
    Array.isArray(frame?.imageExtent) && frame.imageExtent.length === 4
      ? frame.imageExtent
      : projection === "EPSG:3857"
        ? geoBounds3857
        : geoBounds;
  let displayUrl = frame.url;
  if (frame?.url) {
    if (
      activeSource !== SOURCE_HD &&
      activeSource !== SOURCE_SATELLITE &&
      frame?.sourceUrl
    ) {
      try {
        const cleanedNowcastUrl = await getNowcastBlobUrlCached(
          frame.sourceUrl,
        );
        if (cleanedNowcastUrl) {
          frame.url = cleanedNowcastUrl;
          displayUrl = cleanedNowcastUrl;
        }
      } catch (e) {
        console.warn(
          "Nowcast cleanup on-demand failed, fallback to raw frame:",
          e,
        );
      }
    }
    if (
      !Number.isFinite(Number(frame.imageWidth)) ||
      Number(frame.imageWidth) <= 1 ||
      !Number.isFinite(Number(frame.imageHeight)) ||
      Number(frame.imageHeight) <= 1
    ) {
      try {
        const size = await getFrameImageSizeCached(frame.url);
        if (size.width > 1 && size.height > 1) {
          frame.imageWidth = size.width;
          frame.imageHeight = size.height;
        }
      } catch (e) {
        console.warn("Frame size probe failed, fallback to payload size:", e);
      }
    }
    try {
      displayUrl = await getStyledFrameUrl(frame.url);
    } catch (e) {
      console.warn("Styled frame render failed, fallback to raw frame:", e);
      displayUrl = frame.url;
    }
  }
  if (renderToken !== frameRenderToken) return;
  satelliteLayer?.setVisible(false);
  frameLayer?.setVisible(true);
  satelliteReferenceLayer?.setVisible(activeSource === SOURCE_SATELLITE);
  setFrameExtentAndUrl(
    extent,
    displayUrl,
    projection,
    activeSource === SOURCE_SATELLITE,
  );
  timelineEl.value = String(currentFrame);
  timeLabelEl.textContent = frame.timestamp.toLocaleString("ru-RU");
  const layerLabel = getSourceDisplayName(activeSource);
  setStatus(`${layerLabel}: кадр ${currentFrame + 1} / ${frames.length}`);
  updateLightningVisibilityForFrame(true);
  inspectPrecipAtCrosshairCenter().catch(() => {});
}

function stopPlayback(jumpToLast = true) {
  isPlaying = false;
  playbackGeneration++;
  if (timer) {
    clearTimeout(timer);
    timer = null;
  }
  updatePlayButton();
  updateLightningVisibilityForFrame(false);
  if (jumpToLast && frames.length) {
    renderFrame(frames.length - 1);
  }
}

function startPlayback() {
  if (isPlaying || frames.length <= 1) return;
  isPlaying = true;
  playbackGeneration++;
  updateLightningVisibilityForFrame(false);
  const generation = playbackGeneration;
  const startIdx = getPlaybackStartIndex();
  if (currentFrame < startIdx || currentFrame >= frames.length) {
    renderFrame(startIdx).catch((e) => {
      console.warn("Playback start render failed:", e);
    });
  }
  const lastIdx = frames.length - 1;
  schedulePlayback(getPlaybackDelayMs(currentFrame, lastIdx), generation);
  updatePlayButton();
}

function bindControls() {
  playBtnEl.addEventListener("click", () => {
    if (isPlaying) stopPlayback();
    else startPlayback();
  });
  firstBtnEl?.addEventListener("click", () => {
    stopPlayback(false);
    const startIdx = getPlaybackStartIndex();
    renderFrame(startIdx);
  });
  prevBtnEl.addEventListener("click", () => {
    stopPlayback(false);
    renderFrame(currentFrame - 1);
  });
  nextBtnEl.addEventListener("click", () => {
    stopPlayback(false);
    renderFrame(currentFrame + 1);
  });
  lastBtnEl?.addEventListener("click", () => {
    stopPlayback(false);
    if (!frames.length) return;
    renderFrame(frames.length - 1);
  });
  timelineEl.addEventListener("input", () => {
    stopPlayback(false);
    renderFrame(Number(timelineEl.value));
  });
  measureBtnEl?.addEventListener("click", () => {
    setMeasureMode(!measureMode);
    if (measureMode) setDrawMode(false);
  });
  drawBtnEl?.addEventListener("click", () => {
    setDrawMode(!drawMode);
    if (drawMode) setMeasureMode(false);
  });
  crosshairBtnEl?.addEventListener("click", () => {
    setCrosshairMode(!crosshairMode);
  });
  const onLightningToggle = (event) => {
    event?.preventDefault?.();
    event?.stopPropagation?.();
    setLightningOverlayEnabled(!lightningOverlayEnabled);
  };
  lightningToggleBtnEl?.addEventListener("click", onLightningToggle);
  lightningToggleBtnEl?.addEventListener("pointerup", onLightningToggle);
  legendToggleEl?.addEventListener("click", () => {
    legendPanelEl?.classList.toggle("legend-collapsed");
  });
  mobileMenuToggleBtnEl?.addEventListener("click", () => {
    setMobileMenuOpen(true);
  });
  mobileMenuCloseBtnEl?.addEventListener("click", () => {
    setMobileMenuOpen(false);
  });
  mobileMenuBackdropEl?.addEventListener("click", () => {
    setMobileMenuOpen(false);
  });
  window.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") return;
    setMobileMenuOpen(false);
  });
  window.addEventListener("resize", () => {
    if (!isMobileLayout()) {
      setMobileMenuOpen(false);
    }
    syncMapViewportAfterResize();
  });
  window.addEventListener("orientationchange", () => {
    syncMapViewportAfterResize();
  });
  window.visualViewport?.addEventListener?.("resize", () => {
    syncMapViewportAfterResize();
  });
  playbackFrameCountEl?.addEventListener("change", () => {
    if (!frames.length) return;
    if (activeSource !== SOURCE_HD) {
      nowcastFramesByLayer.delete(activeSource);
      switchSource(activeSource).catch((e) => {
        console.warn("Layer reload after playback limit change failed:", e);
      });
      return;
    }
    if (isPlaying) {
      stopPlayback(false);
      const startIdx = getPlaybackStartIndex();
      renderFrame(startIdx);
      startPlayback();
      return;
    }
    const startIdx = getPlaybackStartIndex();
    renderFrame(startIdx);
  });
  sourceLayerSelectEl?.addEventListener("change", async () => {
    setSourceSelectValue(sourceLayerSelectEl.value);
    try {
      await switchSource(sourceLayerSelectEl.value);
    } catch (e) {
      console.warn("Source switch failed:", e);
      setStatus(`Ошибка переключения слоя: ${e.message}`);
    }
  });
  mobileSourceLayerSelectEl?.addEventListener("change", async () => {
    const selected = mobileSourceLayerSelectEl.value || SOURCE_HD;
    setSourceSelectValue(selected);
    try {
      await switchSource(selected);
      if (isMobileLayout()) setMobileMenuOpen(false);
    } catch (e) {
      console.warn("Source switch failed (mobile):", e);
      setStatus(`Ошибка переключения слоя: ${e.message}`);
    }
  });
  satelliteColorModeEl?.addEventListener("change", async () => {
    const nextMode = String(satelliteColorModeEl.value || "rgb");
    await applySatelliteMode(nextMode === "cloudtop");
  });
  mobileSatelliteColorToggleEl?.addEventListener("change", async () => {
    await applySatelliteMode(Boolean(mobileSatelliteColorToggleEl.checked));
  });
}

async function loadData() {
  setStatus("Загрузка данных...");
  const radarResponse = await fetch("/api/radar/latest", { cache: "no-store" });
  if (!radarResponse.ok) throw new Error(`HTTP ${radarResponse.status}`);
  const payload = await radarResponse.json();
  hdFrames = normalizeFrames(payload);
  radars = normalizeRadars(payload);
  if (
    payload.imageSize &&
    Array.isArray(payload.imageSize) &&
    payload.imageSize.length >= 2
  ) {
    radarImageSize = [
      Number(payload.imageSize[0]) || 0,
      Number(payload.imageSize[1]) || 0,
    ];
  }
  if (
    payload.geoBounds &&
    Array.isArray(payload.geoBounds) &&
    payload.geoBounds.length >= 4
  ) {
    geoBounds = payload.geoBounds.slice(0, 4);
  }
  geoBounds3857 = ol.proj.transformExtent(geoBounds, "EPSG:4326", "EPSG:3857");
  for (const f of hdFrames) {
    f.imageExtent = geoBounds.slice();
  }
  if (!hdFrames.length) throw new Error("Пустой список кадров");
}

async function refreshData() {
  try {
    const prevTime =
      activeSource === SOURCE_HD
        ? frames[currentFrame]?.timestamp?.getTime?.()
        : NaN;
    await loadData();
    if (activeSource !== SOURCE_HD) {
      // Не очищаем кэш слоёв при активном non-HD режиме, чтобы повторное
      // переключение было мгновенным. Актуализация идёт периодическим прогревом сервера.
      updateLightningVisibilityForFrame(false);
    }
    upsertRadarLayer();
    if (activeSource !== SOURCE_HD) return;
    frames = hdFrames.slice();
    timelineEl.max = String(Math.max(0, frames.length - 1));
    if (!frames.length) return;

    // Пытаемся сохранить текущий кадр по времени, чтобы не прыгал таймлайн.
    if (Number.isFinite(prevTime)) {
      let nearestIdx = 0;
      let nearestDiff = Infinity;
      for (let i = 0; i < frames.length; i++) {
        const t = frames[i]?.timestamp?.getTime?.();
        if (!Number.isFinite(t)) continue;
        const d = Math.abs(t - prevTime);
        if (d < nearestDiff) {
          nearestDiff = d;
          nearestIdx = i;
        }
      }
      currentFrame = nearestIdx;
    } else if (currentFrame >= frames.length) {
      currentFrame = frames.length - 1;
    }
    renderFrame(currentFrame);
    updateLightningVisibilityForFrame(true);
  } catch (e) {
    console.warn("Refresh data failed:", e);
  }
}

async function init() {
  try {
    setMobileMenuOpen(false);
    syncSatelliteModeControls();
    await loadData();
    frames = hdFrames.slice();
    createMap();
    bindControls();
    setCrosshairMode(false);
    setLightningOverlayEnabled(true);
    renderActiveLegend();
    upsertRadarLayer();
    try {
      await loadNowcastMeta();
      rebuildSourceOptions();
    } catch (e) {
      console.warn("4x4 meta load failed:", e);
    }
    updateSatelliteStylePanelVisibility();
    setStatus(`Готово. Радаров: ${radars.length}`);

    timelineEl.min = "0";
    timelineEl.max = String(Math.max(0, frames.length - 1));
    timelineEl.value = "0";
    await renderFrame(0);
    updateLightningVisibilityForFrame(true);
    if (frames.length > 1) startPlayback();
    else updatePlayButton();
    dataRefreshTimer = setInterval(refreshData, DATA_REFRESH_MS);
  } catch (e) {
    console.error(e);
    setStatus(`Ошибка: ${e.message}`);
  }
}

init();
