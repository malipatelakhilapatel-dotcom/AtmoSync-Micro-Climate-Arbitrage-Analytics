/**
 * AtmoSync — Comprehensive Project Explainer & Operational Console Client
 * Features:
 *  1. Multi-view Tab Router (Overview, Architecture, Kinetics Lab, Live Tower, Team, Case Study)
 *  2. Interactive Architecture Pipeline Flow Simulator & Code Contract Inspector
 *  3. Biological Spoilage Kinetics & Multi-Market Arbitrage Sandbox Calculator
 *  4. Real-time IoT Telematics Streaming, Leaflet GIS Map, Chart.js Visualizations
 *  5. Emergency Reroute Execution & Incident Management
 */

// ============================================================================
// GLOBAL STATE & VARIABLES
// ============================================================================
let autoStreamInterval = null;
let leafletMap = null;
let mapMarkers = {};
let routePolylines = [];
let tempChart = null;
let humChart = null;
let currentFleetData = [];
let currentArbitrageData = [];
let packetSimInterval = null;
let activeSandboxCommodity = 'Avocado';

// Commodity Biological Presets
const COMMODITY_PRESETS = {
  Avocado: {
    temp_opt: 6.0,
    temp_max: 8.0,
    temp_crit: 12.0,
    hum_opt: 90.0,
    base_tts: 120.0,
    typical_price: 185.0,
    default_qty: 12000,
  },
  Banana: {
    temp_opt: 14.0,
    temp_max: 15.0,
    temp_crit: 18.0,
    hum_opt: 90.0,
    base_tts: 96.0,
    typical_price: 48.0,
    default_qty: 15000,
  },
  Tomato: {
    temp_opt: 12.0,
    temp_max: 14.0,
    temp_crit: 18.0,
    hum_opt: 90.0,
    base_tts: 144.0,
    typical_price: 32.0,
    default_qty: 10000,
  },
  Mango: {
    temp_opt: 12.5,
    temp_max: 14.0,
    temp_crit: 17.0,
    hum_opt: 88.0,
    base_tts: 168.0,
    typical_price: 95.0,
    default_qty: 8000,
  },
  Apple: {
    temp_opt: 2.0,
    temp_max: 4.0,
    temp_crit: 8.0,
    hum_opt: 92.0,
    base_tts: 720.0,
    typical_price: 155.0,
    default_qty: 14000,
  },
};

// Architecture Layer Metadata Catalog
const ARCH_LAYERS = {
  layer1: {
    title: 'Edge Telematics: Multi-Container IoT Simulator',
    badge: 'LAYER 1',
    owner: 'Member 1 (IoT & Kafka)',
    tech: 'Python 3.10+, NumPy, Pydantic, State-Machine Models',
    desc: 'Simulates 5 active refrigerated intermodal shipping containers ("reefers") transiting major Indian freight corridors (e.g. Nashik->Mumbai, Hyderabad->Bengaluru). Models realistic thermodynamic heat transfer, compressor valve failures, door-opening spikes, and road surface vibration shocks.',
    inputContract: 'Reefer container physical internal state variables (compressor status, ambient weather)',
    outputContract: 'Pydantic-validated SensorTelemetryEvent payload (Temp, Hum, Vib, GPS, Timestamp)',
    latency: 'Event cycle generated every 2.0 seconds per container',
    resilience: 'Edge memory buffer holds unsent packets during local connectivity dropouts.',
    codeSnippet: `# simulator/iot_simulator.py
event = SensorTelemetryEvent(
    container_id=self.container_id,
    shipment_id=self.shipment_id,
    commodity=self.commodity,
    temperature_c=round(self.temperature_c, 2),
    humidity_pct=round(self.humidity_pct, 2),
    vibration_g=round(self.vibration_g, 3),
    latitude=round(lat, 5),
    longitude=round(lon, 5),
    origin=self.origin,
    destination=self.destination,
    quantity_kg=self.quantity_kg,
    timestamp=datetime.now(timezone.utc)
)`,
  },
  layer2: {
    title: 'Streaming Broker: Apache Kafka (KRaft Mode)',
    badge: 'LAYER 2',
    owner: 'Member 1 (IoT & Kafka)',
    tech: 'Apache Kafka 3.6+, KRaft (ZooKeeper-less), kafka-python, Docker Compose',
    desc: 'Provides low-latency, fault-tolerant message streaming. The resilient producer publishes telemetry keyed by container_id to the partitioned `sensor-data` topic with exponential backoff retries. The streaming consumer validates schemas with Pydantic and dispatches anomalies to the `alerts` topic.',
    inputContract: 'Raw JSON bytes from IoT producer over TCP port 9092',
    outputContract: 'Validated JSON stream to Snowflake micro-batcher + high-priority alert events',
    latency: 'Sub-50ms message delivery between producer and consumer',
    resilience: 'Exponential backoff (up to 5 retries), dead-letter error queue, and local disk staging fallback.',
    codeSnippet: `# kafka/producer.py & consumer.py
# Producer with ACK=all & backoff
producer.send("sensor-data", key=cid.encode(), value=payload)

# Consumer Schema Guard
try:
    validated = SensorTelemetryEvent(**json_payload)
except ValidationError as err:
    log_dead_letter("Corrupted telemetry packet", err)`,
  },
  layer3: {
    title: 'Analytical Warehouse: Snowflake Architecture',
    badge: 'LAYER 3',
    owner: 'Member 2 (Snowflake & dbt)',
    tech: 'Snowflake Cloud Data Warehouse (ATMOSYNC_DB, ATMOSYNC_WH)',
    desc: 'Structured into 3 isolated schemas: RAW, STAGING, and ANALYTICS. Micro-batches of validated telemetry are staged into `RAW.SENSOR_TELEMETRY`. Seed dimensions hold static Indian commodity prices, wholesale terminal coordinates, and highway freight corridors.',
    inputContract: 'JSON micro-batches from Kafka consumer (or batch COPY INTO via external stage)',
    outputContract: 'Columnar analytical tables with automated clustering on container_id and timestamp',
    latency: 'Micro-batches loaded every 5 seconds (or on-demand during simulation)',
    resilience: 'Time Travel retention (1 day), transaction rollback, local JSON file fallback if offline.',
    codeSnippet: `-- snowflake/tables.sql
CREATE TABLE IF NOT EXISTS RAW.SENSOR_TELEMETRY (
    container_id VARCHAR(32) NOT NULL,
    shipment_id VARCHAR(64) NOT NULL,
    commodity VARCHAR(64) NOT NULL,
    temperature_c FLOAT NOT NULL,
    humidity_pct FLOAT NOT NULL,
    vibration_g FLOAT NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    origin VARCHAR(64) NOT NULL,
    destination VARCHAR(64) NOT NULL,
    quantity_kg INTEGER NOT NULL,
    timestamp TIMESTAMP_NTZ NOT NULL,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);`,
  },
  layer4: {
    title: 'Transformation ELT: dbt Core Analytics Engine',
    badge: 'LAYER 4',
    owner: 'Member 2 (Snowflake & dbt)',
    tech: 'dbt Core 1.7+, Modular SQL Models, Jinja Templating',
    desc: 'Transforms raw telemetry into actionable decision matrices. Implements 3-period rolling moving averages, composite Spoilage Risk scoring (0–100), exponential Arrhenius Time-to-Spoilage ($TTS$), and a cross-product matrix calculating Spoilage Arbitrage across reachable Indian markets.',
    inputContract: 'RAW.SENSOR_TELEMETRY + seed_commodity_prices + seed_routes + seed_markets',
    outputContract: 'Gold Marts: `fct_spoilage_arbitrage` and `dim_container`',
    latency: 'Refreshed incrementally every 1 to 5 minutes via Snowflake Tasks or scheduled CLI',
    resilience: 'Automated dbt schema tests (not_null, unique, accepted_values, relationships).',
    codeSnippet: `-- dbt/models/marts/fct_spoilage_arbitrage.sql
SELECT
    container_id,
    commodity,
    spoilage_risk_score,
    time_to_spoilage_hours,
    current_destination,
    recommended_market,
    net_arbitrage_inr,
    CASE 
        WHEN net_arbitrage_inr > 0 AND transit_hours <= time_to_spoilage_hours 
             AND spoilage_risk_score >= 50 THEN 'EXECUTE_REROUTE'
        WHEN net_arbitrage_inr > 0 THEN 'OPPORTUNITY_IDENTIFIED'
        ELSE 'MAINTAIN_ROUTE'
    END AS dispatch_directive
FROM intermediate_arbitrage_calc;`,
  },
  layer5: {
    title: 'BI & Trading Control Tower: Superset & Console',
    badge: 'LAYER 5',
    owner: 'Member 3 (Superset BI & Docs)',
    tech: 'Apache Superset, Leaflet GIS, Chart.js, REST API, WebSocket/Polling',
    desc: 'Delivers actionable intelligence directly to commodity traders and fleet dispatchers. Displays geospatial container corridors, multi-series thermal profiles, risk leaderboards, and an interactive Arbitrage Decision Matrix with 1-click rerouting authorization.',
    inputContract: 'Snowflake Gold Mart `fct_spoilage_arbitrage` / Python REST Server `/api/data`',
    outputContract: 'Interactive visual dashboards and dispatch webhook notifications (Slack/Email)',
    latency: 'Real-time interactive dashboard refresh with auto-streaming',
    resilience: 'Works 100% offline with standalone Python simulation server or connected to cloud warehouse.',
    codeSnippet: `// superset/dashboard_export.json & web/app.js
// Dispatcher executes instant reroute directive
fetch('/api/reroute', {
  method: 'POST',
  body: JSON.stringify({ container_id: 'CONT_001' })
}).then(res => res.json());`,
  },
};

// ============================================================================
// FORMATTING UTILITIES
// ============================================================================
const formatINR = (val) => {
  return new Intl.NumberFormat('en-IN', {
    style: 'currency',
    currency: 'INR',
    maximumFractionDigits: 0,
  }).format(val || 0);
};

const getRiskClass = (tier) => {
  switch (tier) {
    case 'Critical': return 'badge-danger';
    case 'High': return 'badge-warning';
    case 'Moderate': return 'badge-warning';
    default: return 'badge-normal';
  }
};

const getProgressColor = (tier) => {
  switch (tier) {
    case 'Critical': return 'fill-danger';
    case 'High': return 'fill-amber';
    case 'Moderate': return 'fill-amber';
    default: return 'fill-emerald';
  }
};

// ============================================================================
// TAB & SLIDE PRESENTATION CONTROLLER
// ============================================================================
const SLIDE_ORDER = [
  'tab-overview',
  'tab-architecture',
  'tab-math-lab',
  'tab-live-console',
  'tab-team',
  'tab-case-study',
  'tab-bi-replica'
];

function getCurrentSlideIndex() {
  const currentTab = document.querySelector('.nav-tab.active')?.getAttribute('data-tab') || 'tab-overview';
  const idx = SLIDE_ORDER.indexOf(currentTab);
  return idx >= 0 ? idx : 0;
}

function nextSlide() {
  const idx = getCurrentSlideIndex();
  const nextIdx = (idx + 1) % SLIDE_ORDER.length;
  switchTab(SLIDE_ORDER[nextIdx]);
}

function prevSlide() {
  const idx = getCurrentSlideIndex();
  const prevIdx = (idx - 1 + SLIDE_ORDER.length) % SLIDE_ORDER.length;
  switchTab(SLIDE_ORDER[prevIdx]);
}

// Global keyboard arrow navigation for slide presentation mode
document.addEventListener('keydown', (e) => {
  if (['INPUT', 'TEXTAREA', 'SELECT'].includes(document.activeElement?.tagName)) return;
  if (e.key === 'ArrowRight') {
    e.preventDefault();
    nextSlide();
  } else if (e.key === 'ArrowLeft') {
    e.preventDefault();
    prevSlide();
  }
});

function switchTab(targetTabId) {
  // Ensure target exists, fallback gracefully if not
  if (!document.getElementById(targetTabId)) {
    console.warn(`Target tab ${targetTabId} not found, defaulting to tab-overview`);
    targetTabId = 'tab-overview';
  }

  // Update Tab buttons
  document.querySelectorAll('.nav-tab').forEach((tab) => {
    tab.classList.toggle('active', tab.getAttribute('data-tab') === targetTabId);
  });

  // Update Tab Panes
  document.querySelectorAll('.tab-pane').forEach((pane) => {
    pane.classList.toggle('active', pane.id === targetTabId);
  });

  // Update URL hash without jumping
  if (history.replaceState) {
    history.replaceState(null, null, `#${targetTabId.replace('tab-', '')}`);
  }

  // Smooth scroll to top of view
  window.scrollTo({ top: 0, behavior: 'smooth' });

  // If switching to Live Console, trigger Leaflet & Chart size invalidations safely
  if (targetTabId === 'tab-live-console') {
    setTimeout(() => {
      try {
        if (leafletMap && typeof leafletMap.invalidateSize === 'function') {
          leafletMap.invalidateSize();
        }
        if (tempChart && typeof tempChart.resize === 'function') tempChart.resize();
        if (humChart && typeof humChart.resize === 'function') humChart.resize();
      } catch (err) {
        console.warn('Map/chart resize error:', err);
      }
    }, 150);
  }

  // If switching to Math Lab, trigger calculation
  if (targetTabId === 'tab-math-lab') {
    updateMathSandbox();
  }

  // If switching to Executive BI View, initialize/update BI replica charts
  if (targetTabId === 'tab-bi-replica') {
    setTimeout(() => {
      try {
        initPbiCharts();
        updatePbiTable(currentArbitrageData, currentFleetData);
        if (pbiChartArbitrage && typeof pbiChartArbitrage.resize === 'function') pbiChartArbitrage.resize();
        if (pbiChartDegradation && typeof pbiChartDegradation.resize === 'function') pbiChartDegradation.resize();
        if (pbiChartMandi && typeof pbiChartMandi.resize === 'function') pbiChartMandi.resize();
      } catch (err) {
        console.warn('PBI chart resize error:', err);
      }
    }, 150);
  }
}

// Initial Tab Route from Hash
function initTabRouter() {
  const hash = window.location.hash.replace('#', '');
  if (hash) {
    const targetId = `tab-${hash}`;
    if (document.getElementById(targetId)) {
      switchTab(targetId);
      return;
    }
  }
  // Default to overview
  switchTab('tab-overview');
}

// ============================================================================
// ARCHITECTURE PIPELINE FLOW & PACKET SIMULATOR
// ============================================================================
function selectLayer(layerId) {
  const layer = ARCH_LAYERS[layerId];
  if (!layer) return;

  // Highlight active stage card
  document.querySelectorAll('.arch-stage-card').forEach((card) => {
    card.classList.toggle('active-stage', card.id === `stage-${layerId}`);
  });

  // Update Detail Card Header
  const titleEl = document.getElementById('layer-detail-title');
  titleEl.innerHTML = `<span class="badge ${layerId === 'layer2' ? 'badge-indigo' : layerId === 'layer4' ? 'badge-warning' : layerId === 'layer5' ? 'badge-normal' : 'badge-cyan'}">${layer.badge}</span> ${layer.title}`;

  document.getElementById('layer-detail-owner').textContent = `Owner: ${layer.owner}`;

  // Update Detail Card Body
  const bodyEl = document.getElementById('layer-detail-body');
  bodyEl.innerHTML = `
    <div class="layer-detail-row">
      <span class="layer-detail-label">Technology &amp; Dependencies</span>
      <span class="layer-detail-value" style="font-family: var(--font-mono); color: var(--color-cerulean);">${layer.tech}</span>
    </div>
    <div class="layer-detail-row">
      <span class="layer-detail-label">Architectural Role</span>
      <span class="layer-detail-value">${layer.desc}</span>
    </div>
    <div class="layer-detail-row">
      <span class="layer-detail-label">Data Contracts</span>
      <span class="layer-detail-value"><strong>Input:</strong> ${layer.inputContract}</span>
      <span class="layer-detail-value" style="margin-top: 2px;"><strong>Output:</strong> ${layer.outputContract}</span>
    </div>
    <div class="layer-detail-row">
      <span class="layer-detail-label">Performance SLA &amp; Fault Tolerance</span>
      <span class="layer-detail-value"><strong>Latency:</strong> ${layer.latency}</span>
      <span class="layer-detail-value" style="margin-top: 2px;"><strong>Resilience:</strong> ${layer.resilience}</span>
    </div>
  `;

  // Update Code block to show sample snippet for this layer
  document.getElementById('contract-code-display').innerHTML = `<code class="language-code">${escapeHtml(layer.codeSnippet)}</code>`;
}

function escapeHtml(text) {
  const map = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' };
  return text.replace(/[&<>"']/g, (m) => map[m]);
}

function showContractView(type) {
  document.querySelectorAll('.contract-tabs .btn').forEach((b) => b.classList.remove('active'));
  const btn = document.getElementById(`btn-contract-${type}`);
  if (btn) btn.classList.add('active');

  const display = document.getElementById('contract-code-display');
  if (type === 'json') {
    display.innerHTML = `<code class="language-json">{
  "container_id": "CONT_001",
  "shipment_id": "SHIP_7894",
  "commodity": "Avocado",
  "temperature_c": 14.2,
  "humidity_pct": 91.2,
  "vibration_g": 0.18,
  "latitude": 19.3412,
  "longitude": 73.4521,
  "origin": "Nashik",
  "destination": "Mumbai",
  "quantity_kg": 12000,
  "timestamp": "2026-10-01T14:48:31.156Z"
}</code>`;
  } else if (type === 'sql') {
    display.innerHTML = `<code class="language-sql">-- Snowflake DDL for RAW Staging
CREATE OR REPLACE TABLE RAW.SENSOR_TELEMETRY (
    container_id VARCHAR(32) NOT NULL,
    shipment_id VARCHAR(64) NOT NULL,
    commodity VARCHAR(64) NOT NULL,
    temperature_c FLOAT NOT NULL,
    humidity_pct FLOAT NOT NULL,
    vibration_g FLOAT NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    origin VARCHAR(64) NOT NULL,
    destination VARCHAR(64) NOT NULL,
    quantity_kg INTEGER NOT NULL,
    timestamp TIMESTAMP_NTZ NOT NULL,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);</code>`;
  } else if (type === 'dbt') {
    display.innerHTML = `<code class="language-sql">-- dbt Gold Mart: fct_spoilage_arbitrage.sql
SELECT
    container_id,
    commodity,
    spoilage_risk_score,
    time_to_spoilage_hours,
    current_destination,
    recommended_market,
    diversion_cost_inr,
    net_arbitrage_inr,
    directive
FROM {{ ref('int_market_options') }}
WHERE is_optimal_reroute = TRUE;</code>`;
  }
}

function appendPipelineLog(timeStr, message, type = 'info') {
  const logContainer = document.getElementById('pipeline-live-logs');
  if (!logContainer) return;
  const entry = document.createElement('div');
  entry.className = `log-entry log-${type}`;
  entry.innerHTML = `<span class="log-time">[${timeStr}]</span> ${message}`;
  logContainer.appendChild(entry);
  logContainer.scrollTop = logContainer.scrollHeight;
}

function triggerPacketSimulation() {
  resetPacketSimulation();
  const stages = ['layer1', 'layer2', 'layer3', 'layer4', 'layer5'];
  let currentStep = 0;

  appendPipelineLog(new Date().toTimeString().split(' ')[0], '▶ Commencing live telematics packet injection...', 'info');

  packetSimInterval = setInterval(() => {
    if (currentStep >= stages.length) {
      clearInterval(packetSimInterval);
      packetSimInterval = null;
      appendPipelineLog(new Date().toTimeString().split(' ')[0], '✓ End-to-end pipeline trace complete: Directive EXECUTE_REROUTE issued.', 'success');
      return;
    }

    const currentLayer = stages[currentStep];
    selectLayer(currentLayer);

    // Pulse current stage card
    const stageEl = document.getElementById(`stage-${currentLayer}`);
    if (stageEl) {
      stageEl.classList.add('pulsing');
      setTimeout(() => stageEl.classList.remove('pulsing'), 700);
    }

    // Pulse connector if not last
    if (currentStep < 4) {
      const connEl = document.getElementById(`conn-${currentStep + 1}`);
      if (connEl) {
        connEl.classList.add('pulsing');
        setTimeout(() => connEl.classList.remove('pulsing'), 700);
      }
    }

    const now = new Date().toTimeString().split(' ')[0];
    if (currentStep === 0) {
      appendPipelineLog(now, 'Layer 1: CONT_001 IoT sensor generated telematics: 14.2°C, 91.2% RH, 0.18G', 'info');
    } else if (currentStep === 1) {
      appendPipelineLog(now, 'Layer 2: Kafka broker received packet on `sensor-data`. Pydantic PASS. Alert dispatched to `alerts` topic!', 'alert');
    } else if (currentStep === 2) {
      appendPipelineLog(now, 'Layer 3: Snowflake micro-batch staging table RAW.SENSOR_TELEMETRY committed 1 record.', 'info');
    } else if (currentStep === 3) {
      appendPipelineLog(now, 'Layer 4: dbt models executed. Risk Score=67.4 (Critical), TTS=20.0h. Pune Arbitrage=+₹283,189.', 'alert');
    } else if (currentStep === 4) {
      appendPipelineLog(now, 'Layer 5: Decision Tower triggered incident banner: Reroute CONT_001 to Pune Terminal.', 'critical');
    }

    currentStep++;
  }, 1000);
}

function resetPacketSimulation() {
  if (packetSimInterval) {
    clearInterval(packetSimInterval);
    packetSimInterval = null;
  }
  document.querySelectorAll('.arch-stage-card').forEach((c) => c.classList.remove('pulsing'));
  document.querySelectorAll('.arch-connector').forEach((c) => c.classList.remove('pulsing'));
}

// ============================================================================
// KINETICS & ARBITRAGE MATHEMATICAL SANDBOX
// ============================================================================
function setSandboxCommodity(name) {
  activeSandboxCommodity = name;
  document.querySelectorAll('#comm-chips .chip').forEach((c) => {
    c.classList.toggle('active', c.textContent.includes(name));
  });

  const preset = COMMODITY_PRESETS[name];
  if (!preset) return;

  // Set safe label
  const safeLabel = document.getElementById('label-safe-temp');
  if (safeLabel) {
    safeLabel.textContent = `Safe: ${preset.temp_opt.toFixed(1)}°C – ${preset.temp_max.toFixed(1)}°C`;
  }

  updateMathSandbox();
}

function loadMathPreset(type) {
  const tempSlider = document.getElementById('slider-temp');
  const humSlider = document.getElementById('slider-hum');
  const vibSlider = document.getElementById('slider-vib');
  const alertsSlider = document.getElementById('slider-alerts');
  const distSlider = document.getElementById('slider-dist');

  if (type === 'optimal') {
    setSandboxCommodity('Avocado');
    tempSlider.value = 6.2;
    humSlider.value = 90.0;
    vibSlider.value = 0.08;
    alertsSlider.value = 0;
    distSlider.value = 145;
  } else if (type === 'mild') {
    setSandboxCommodity('Banana');
    tempSlider.value = 16.5;
    humSlider.value = 85.0;
    vibSlider.value = 0.15;
    alertsSlider.value = 1;
    distSlider.value = 210;
  } else if (type === 'cont001') {
    setSandboxCommodity('Avocado');
    tempSlider.value = 14.2;
    humSlider.value = 91.5;
    vibSlider.value = 0.18;
    alertsSlider.value = 3;
    distSlider.value = 145;
  } else if (type === 'catastrophic') {
    setSandboxCommodity('Tomato');
    tempSlider.value = 22.0;
    humSlider.value = 98.0;
    vibSlider.value = 0.45;
    alertsSlider.value = 5;
    distSlider.value = 180;
  }

  updateMathSandbox();
}

function updateMathSandbox() {
  const tempSlider = document.getElementById('slider-temp');
  const humSlider = document.getElementById('slider-hum');
  const vibSlider = document.getElementById('slider-vib');
  const alertsSlider = document.getElementById('slider-alerts');
  const distSlider = document.getElementById('slider-dist');

  if (!tempSlider) return;

  const tempVal = parseFloat(tempSlider.value);
  const humVal = parseFloat(humSlider.value);
  const vibVal = parseFloat(vibSlider.value);
  const alertsVal = parseInt(alertsSlider.value, 10);
  const distVal = parseFloat(distSlider.value);

  // Update slider display tags
  document.getElementById('val-slider-temp').textContent = `${tempVal.toFixed(1)} °C`;
  document.getElementById('val-slider-hum').textContent = `${humVal.toFixed(1)} %`;
  document.getElementById('val-slider-vib').textContent = `${vibVal.toFixed(2)} G`;
  document.getElementById('val-slider-alerts').textContent = `${alertsVal} Alerts`;
  document.getElementById('val-slider-dist').textContent = `${distVal} km`;

  // Dynamic visual track fill for range sliders
  [
    { id: 'slider-temp', min: 0, max: 25 },
    { id: 'slider-hum', min: 60, max: 100 },
    { id: 'slider-vib', min: 0.05, max: 0.8 },
    { id: 'slider-alerts', min: 0, max: 5 },
    { id: 'slider-dist', min: 50, max: 600 },
  ].forEach((item) => {
    const el = document.getElementById(item.id);
    if (el) {
      const val = parseFloat(el.value);
      const pct = Math.max(0, Math.min(100, ((val - item.min) / (item.max - item.min)) * 100));
      const color = item.id === 'slider-temp' && val > 12 ? '#ef4444' : item.id === 'slider-temp' && val > 8 ? '#f59e0b' : '#00d2ff';
      el.style.background = `linear-gradient(to right, ${color} 0%, ${color} ${pct}%, rgba(255, 255, 255, 0.12) ${pct}%, rgba(255, 255, 255, 0.12) 100%)`;
    }
  });

  const specs = COMMODITY_PRESETS[activeSandboxCommodity];

  // 1. Spoilage Penalties (as formulated in docs/business_logic.md)
  const tempPenalty = Math.min(
    45.0,
    Math.max(0.0, ((tempVal - specs.temp_opt) / (specs.temp_crit - specs.temp_opt)) * 45.0)
  );

  const humPenalty = Math.min(
    20.0,
    Math.max(0.0, (Math.abs(humVal - 90.0) / 20.0) * 20.0)
  );

  const vibPenalty = Math.min(
    15.0,
    Math.max(0.0, ((vibVal - 0.10) / 0.35) * 15.0)
  );

  const expPenalty = Math.min(20.0, alertsVal * 4.0);

  const totalRisk = Math.min(
    100.0,
    Math.max(0.0, Math.round((tempPenalty + humPenalty + vibPenalty + expPenalty) * 10) / 10)
  );

  // 2. Risk Level Categorization
  let riskLevel = 'Low';
  let badgeClass = 'badge-normal';
  if (totalRisk >= 75.0) {
    riskLevel = 'Critical';
    badgeClass = 'badge-danger';
  } else if (totalRisk >= 50.0) {
    riskLevel = 'High';
    badgeClass = 'badge-warning';
  } else if (totalRisk >= 25.0) {
    riskLevel = 'Moderate';
    badgeClass = 'badge-warning';
  }

  // 3. Dynamic Time-to-Spoilage (TTS) Hours
  const decayFactor = Math.max(0.05, 1.0 - totalRisk / 100.0);
  const ttsHours = Math.max(1.0, Math.round(specs.base_tts * Math.pow(decayFactor, 1.6) * 10) / 10);

  // Update Penalties DOM
  document.getElementById('val-p-temp').textContent = `${tempPenalty.toFixed(1)} / 45 pts`;
  document.getElementById('bar-p-temp').style.width = `${(tempPenalty / 45.0) * 100}%`;

  document.getElementById('val-p-hum').textContent = `${humPenalty.toFixed(1)} / 20 pts`;
  document.getElementById('bar-p-hum').style.width = `${(humPenalty / 20.0) * 100}%`;

  document.getElementById('val-p-vib').textContent = `${vibPenalty.toFixed(1)} / 15 pts`;
  document.getElementById('bar-p-vib').style.width = `${(vibPenalty / 15.0) * 100}%`;

  document.getElementById('val-p-exp').textContent = `${expPenalty.toFixed(1)} / 20 pts`;
  document.getElementById('bar-p-exp').style.width = `${(expPenalty / 20.0) * 100}%`;

  // Update Risk & TTS DOM
  document.getElementById('val-total-risk').textContent = `${totalRisk.toFixed(1)} / 100`;
  document.getElementById('val-risk-category').textContent = `${riskLevel} Risk Tier`;

  const riskBadge = document.getElementById('sandbox-risk-badge');
  riskBadge.className = `badge ${badgeClass}`;
  riskBadge.textContent = `${riskLevel} Risk`;

  document.getElementById('val-dynamic-tts').textContent = `${ttsHours.toFixed(1)}h`;

  // 4. Financial Arbitrage Calculations
  const qty = specs.default_qty;
  const currPrice = specs.typical_price;
  const candPrice = Math.round(currPrice * 1.03); // slight premium or spot variance

  const scheduledTransitHours = 8.5; // Mumbai standard route
  const candTransitHours = Math.max(1.0, Math.round((distVal / 50.0) * 10) / 10); // ~50 km/h truck avg

  // Scheduled route loss
  let currLossPct = 0.0;
  if (scheduledTransitHours > ttsHours) {
    currLossPct = 0.80; // Total dock rejection write-off
  } else {
    currLossPct = Math.min(0.70, (totalRisk / 100.0) * 0.40);
  }

  // Candidate diversion loss
  let candLossPct = 0.85;
  const isViable = candTransitHours <= ttsHours;
  if (isViable) {
    if (candTransitHours <= ttsHours * 0.5) {
      candLossPct = 0.10;
    } else {
      candLossPct = 0.25;
    }
  }

  const rerouteHaulageCost = Math.round(distVal * 35.0 + 600.0);
  const currExpValue = Math.round(qty * currPrice * (1.0 - currLossPct));
  const rerouteExpValue = Math.round(qty * candPrice * (1.0 - candLossPct));
  const netArbitrage = Math.round(rerouteExpValue - rerouteHaulageCost - currExpValue);

  // Update Financial DOM
  document.getElementById('val-curr-outcome').textContent = formatINR(currExpValue);
  document.getElementById('val-curr-loss-note').textContent =
    scheduledTransitHours > ttsHours ? '80% Dock Rejection Write-Off' : `${(currLossPct * 100).toFixed(0)}% Degradation Loss`;

  document.getElementById('val-reroute-outcome').textContent = formatINR(rerouteExpValue);
  document.getElementById('val-reroute-cost').textContent = formatINR(rerouteHaulageCost);

  const netArbEl = document.getElementById('val-net-arbitrage');
  const dirBadge = document.getElementById('sandbox-directive-badge');

  if (netArbitrage > 0 && isViable && totalRisk >= 50.0) {
    netArbEl.className = 'net-arb-value text-emerald';
    netArbEl.textContent = `+${formatINR(netArbitrage)}`;
    dirBadge.className = 'badge badge-normal';
    dirBadge.textContent = 'EXECUTE_REROUTE';
  } else if (netArbitrage > 0) {
    netArbEl.className = 'net-arb-value text-cerulean';
    netArbEl.textContent = `+${formatINR(netArbitrage)}`;
    dirBadge.className = 'badge badge-cyan';
    dirBadge.textContent = 'OPPORTUNITY_IDENTIFIED';
  } else {
    netArbEl.className = 'net-arb-value text-dim';
    netArbEl.textContent = formatINR(netArbitrage);
    dirBadge.className = 'badge badge-warning';
    dirBadge.textContent = 'MAINTAIN_ROUTE';
  }
}

// ============================================================================
// LIVE OPERATIONAL CONSOLE: MAP, CHARTS & API INTEGRATION
// ============================================================================
function initMap() {
  if (leafletMap) return;

  const mapContainer = document.getElementById('geo-map');
  if (!mapContainer) return;

  // Center on Central/Western India freight corridor
  leafletMap = L.map('geo-map', {
    center: [18.8, 76.5],
    zoom: 6,
    zoomControl: true,
  });

  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; OpenStreetMap contributors',
    maxZoom: 18,
  }).addTo(leafletMap);
}

function updateMap(fleet, markets) {
  if (!leafletMap) return;

  // Clear previous layers
  Object.values(mapMarkers).forEach((marker) => leafletMap.removeLayer(marker));
  mapMarkers = {};
  routePolylines.forEach((line) => leafletMap.removeLayer(line));
  routePolylines = [];

  // Draw Market Nodes
  if (markets) {
    Object.entries(markets).forEach(([name, coords]) => {
      const circle = L.circleMarker([coords.lat, coords.lon], {
        radius: 6,
        fillColor: '#38bdf8',
        color: '#0284c7',
        weight: 1.5,
        opacity: 0.8,
        fillOpacity: 0.6,
      }).addTo(leafletMap);

      circle.bindTooltip(`<strong>${name} Ag Terminal</strong>`, {
        permanent: false,
        direction: 'top',
      });
      routePolylines.push(circle);
    });
  }

  // Draw Container Locations & Routes
  fleet.forEach((c) => {
    let pinColor = '#10b981';
    if (c.risk_level === 'Critical') pinColor = '#ef4444';
    else if (c.risk_level === 'High' || c.risk_level === 'Moderate') pinColor = '#f59e0b';

    const customIcon = L.divIcon({
      className: 'custom-container-pin',
      html: `
        <div style="
          width: 22px; 
          height: 22px; 
          background: ${pinColor}; 
          border: 2px solid #ffffff; 
          border-radius: 50%; 
          display: flex; 
          align-items: center; 
          justify-content: center;
          box-shadow: 0 0 14px ${pinColor};
          color: #000;
          font-weight: 800;
          font-size: 10px;
          font-family: var(--font-mono);
        ">
          ${c.container_id.replace('CONT_00', '')}
        </div>
      `,
      iconSize: [22, 22],
      iconAnchor: [11, 11],
    });

    const marker = L.marker([c.latitude, c.longitude], { icon: customIcon }).addTo(leafletMap);

    const popupHtml = `
      <div style="font-family: var(--font-body); font-size: 12px; min-width: 170px; line-height: 1.4;">
        <strong style="color: #0284c7; font-size: 13px;">${c.container_id} (${c.commodity})</strong><br/>
        <span>Route: <strong>${c.origin} &rarr; ${c.destination}</strong></span><br/>
        <span>Temp: <strong>${c.temperature_c.toFixed(1)}°C</strong> | Hum: <strong>${c.humidity_pct.toFixed(1)}%</strong></span><br/>
        <span>Risk: <strong style="color: ${pinColor}">${c.spoilage_risk_score} (${c.risk_level})</strong></span><br/>
        <span>Shelf Life (TTS): <strong>${c.time_to_spoilage_hours}h</strong></span><br/>
        <span>Action: <strong>${c.action}</strong></span>
      </div>
    `;
    marker.bindPopup(popupHtml);
    mapMarkers[c.container_id] = marker;

    // Draw route vector to destination
    if (markets && markets[c.destination.replace(' (Rerouted)', '')]) {
      const destCoords = markets[c.destination.replace(' (Rerouted)', '')];
      const poly = L.polyline(
        [
          [c.latitude, c.longitude],
          [destCoords.lat, destCoords.lon],
        ],
        {
          color: c.is_rerouted ? '#10b981' : pinColor,
          weight: c.is_rerouted ? 3 : 2,
          dashArray: c.is_rerouted ? null : '4, 6',
          opacity: 0.65,
        }
      ).addTo(leafletMap);
      routePolylines.push(poly);
    }
  });
}

function initCharts() {
  const tempCtx = document.getElementById('tempChart');
  const humCtx = document.getElementById('humChart');

  if (!tempCtx || !humCtx) return;

  const baseChartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 350 },
    plugins: {
      legend: {
        position: 'top',
        labels: {
          color: '#94a3b8',
          font: { family: 'Outfit', size: 11, weight: '500' },
          boxWidth: 10,
          boxHeight: 10,
        },
      },
      tooltip: {
        backgroundColor: 'rgba(15, 23, 42, 0.95)',
        titleColor: '#f8fafc',
        bodyColor: '#38bdf8',
        borderColor: 'rgba(255, 255, 255, 0.1)',
        borderWidth: 1,
        bodyFont: { family: 'JetBrains Mono', size: 12 },
      },
    },
    scales: {
      x: {
        grid: { color: 'rgba(255, 255, 255, 0.04)' },
        ticks: { color: '#64748b', font: { family: 'JetBrains Mono', size: 10 } },
      },
      y: {
        grid: { color: 'rgba(255, 255, 255, 0.06)' },
        ticks: { color: '#64748b', font: { family: 'JetBrains Mono', size: 10 } },
      },
    },
  };

  tempChart = new Chart(tempCtx, {
    type: 'line',
    data: { labels: [], datasets: [] },
    options: {
      ...baseChartOptions,
      scales: {
        ...baseChartOptions.scales,
        y: {
          ...baseChartOptions.scales.y,
          title: { display: true, text: 'Temperature (°C)', color: '#64748b' },
          suggestedMin: 0,
          suggestedMax: 20,
        },
      },
    },
  });

  humChart = new Chart(humCtx, {
    type: 'line',
    data: { labels: [], datasets: [] },
    options: {
      ...baseChartOptions,
      scales: {
        ...baseChartOptions.scales,
        y: {
          ...baseChartOptions.scales.y,
          title: { display: true, text: 'Relative Humidity (% RH)', color: '#64748b' },
          suggestedMin: 80,
          suggestedMax: 100,
        },
      },
    },
  });
}

function updateCharts(history) {
  if (!tempChart || !humChart || !history) return;

  const colors = {
    CONT_001: '#ef4444',
    CONT_002: '#f59e0b',
    CONT_003: '#38bdf8',
    CONT_004: '#6366f1',
    CONT_005: '#10b981',
  };

  const cids = Object.keys(history);
  if (cids.length === 0) return;

  const maxLen = Math.max(...cids.map((id) => history[id].length));
  const labels = Array.from({ length: maxLen }, (_, i) => `T-${maxLen - 1 - i}m`);

  tempChart.data.labels = labels;
  tempChart.data.datasets = cids.map((id) => ({
    label: `${id}`,
    data: history[id].map((h) => h.temp),
    borderColor: colors[id] || '#00d2ff',
    backgroundColor: 'transparent',
    borderWidth: id === 'CONT_001' ? 2.5 : 1.5,
    tension: 0.25,
    pointRadius: 2,
  }));
  tempChart.update();

  humChart.data.labels = labels;
  humChart.data.datasets = cids.map((id) => ({
    label: `${id}`,
    data: history[id].map((h) => h.hum),
    borderColor: colors[id] || '#00d2ff',
    backgroundColor: 'transparent',
    borderWidth: 1.5,
    tension: 0.25,
    pointRadius: 2,
  }));
  humChart.update();
}

function updateLeaderboard(fleet) {
  const container = document.getElementById('leaderboard-container');
  if (!container) return;

  const sortedFleet = [...fleet].sort((a, b) => b.spoilage_risk_score - a.spoilage_risk_score);

  container.innerHTML = sortedFleet
    .map((c) => {
      const riskClass = getRiskClass(c.risk_level);
      const progColor = getProgressColor(c.risk_level);

      return `
      <div class="leaderboard-item">
        <div class="item-left">
          <span class="container-badge">${c.container_id}</span>
          <div class="item-commodity">
            <strong>${c.commodity}</strong> • ${c.destination}
          </div>
        </div>
        <div class="item-right">
          <div class="risk-progress-wrap">
            <div class="progress-track">
              <div class="progress-fill ${progColor}" style="width: ${c.spoilage_risk_score}%;"></div>
            </div>
            <div class="progress-label">
              <span>Risk: <strong>${c.spoilage_risk_score}</strong></span>
              <span>TTS: <strong>${c.time_to_spoilage_hours}h</strong></span>
            </div>
          </div>
          <span class="badge ${riskClass}">${c.risk_level}</span>
        </div>
      </div>
    `;
    })
    .join('');
}

function updateArbitrageTable(arbitrageList, fleet) {
  const tbody = document.getElementById('arbitrage-table-body');
  if (!tbody) return;

  currentArbitrageData = arbitrageList;
  const filterText = (document.getElementById('table-search')?.value || '').toLowerCase();

  const fleetMap = {};
  fleet.forEach((f) => (fleetMap[f.container_id] = f));

  tbody.innerHTML = arbitrageList
    .filter((a) => {
      if (!filterText) return true;
      return (
        a.container_id.toLowerCase().includes(filterText) ||
        a.commodity.toLowerCase().includes(filterText) ||
        a.recommended_market.toLowerCase().includes(filterText)
      );
    })
    .map((a) => {
      const container = fleetMap[a.container_id] || {};
      const riskTier = container.risk_level || 'Low';
      const riskClass = getRiskClass(riskTier);

      let directiveBadge = 'badge-normal';
      if (a.directive === 'EXECUTE_REROUTE') directiveBadge = 'badge-danger';
      else if (a.directive === 'OPPORTUNITY_IDENTIFIED') directiveBadge = 'badge-warning';
      else if (a.directive === 'REROUTED') directiveBadge = 'badge-normal';

      return `
      <tr>
        <td><strong>${a.container_id}</strong></td>
        <td>${a.commodity}</td>
        <td><code>${container.temperature_c?.toFixed(1) || 0}°C / ${container.humidity_pct?.toFixed(1) || 0}%</code></td>
        <td><span class="badge ${riskClass}">${riskTier}</span></td>
        <td><strong>${container.time_to_spoilage_hours || 'N/A'}h</strong></td>
        <td>${a.current_destination}</td>
        <td class="text-danger">${formatINR(a.potential_loss)}</td>
        <td><strong class="text-cyan">${a.recommended_market}</strong></td>
        <td>${formatINR(a.diversion_cost)}</td>
        <td class="${a.net_arbitrage_inr > 0 ? 'text-emerald' : 'text-muted'}">
          <strong>${a.net_arbitrage_inr > 0 ? '+' : ''}${formatINR(a.net_arbitrage_inr)}</strong>
        </td>
        <td>
          <span class="badge ${directiveBadge}">${a.directive}</span>
          ${
            a.directive === 'EXECUTE_REROUTE'
              ? `<button class="btn btn-danger-glow btn-sm" style="margin-left: 8px;" onclick="executeReroute('${a.container_id}')">Reroute</button>`
              : ''
          }
        </td>
      </tr>
    `;
    })
    .join('');
}

function updateKPIs(kpis) {
  if (!kpis) return;
  document.getElementById('kpi-val-containers').textContent = kpis.total_containers;
  document.getElementById('kpi-val-at-risk').textContent = kpis.at_risk_count;
  document.getElementById('kpi-val-critical').textContent = kpis.critical_count;
  document.getElementById('kpi-val-temp').textContent = `${kpis.average_temp_c}°C`;
  document.getElementById('kpi-val-hum').textContent = `${kpis.average_humidity_pct}%`;
  document.getElementById('kpi-val-loss-avoided').textContent = formatINR(kpis.total_loss_avoided_inr);
  document.getElementById('kpi-val-arbitrage').textContent = formatINR(kpis.total_potential_arbitrage_inr);
}

function updateIncidentBanner(fleet, arbitrage) {
  const banner = document.getElementById('incident-banner');
  const bannerRisk = document.getElementById('banner-risk');
  const bannerTts = document.getElementById('banner-tts');
  const bannerBtn = document.getElementById('banner-reroute-btn');

  if (!banner) return;

  const cont1 = fleet.find((c) => c.container_id === 'CONT_001');
  const arb1 = arbitrage.find((a) => a.container_id === 'CONT_001');

  if (cont1 && cont1.is_rerouted) {
    banner.style.display = 'flex';
    banner.style.borderColor = '#10b981';
    banner.style.background = 'rgba(16, 185, 129, 0.12)';
    document.getElementById('banner-title').innerHTML =
      `✅ CONT_001 DIVERSION EXECUTED: REROUTED TO PUNE TERMINAL`;
    document.getElementById('banner-desc').innerHTML =
      `Consignment of 12,000 kg Avocados diverted. Safe delivery guaranteed within remaining shelf life (${cont1.time_to_spoilage_hours}h). <strong>₹283,189</strong> net arbitrage preserved!`;
    if (bannerBtn) {
      bannerBtn.disabled = true;
      bannerBtn.textContent = 'Diversion Active';
      bannerBtn.className = 'btn btn-outline';
    }
  } else if (cont1 && cont1.risk_level === 'Critical') {
    banner.style.display = 'flex';
    if (bannerRisk) bannerRisk.textContent = `${cont1.spoilage_risk_score}/100`;
    if (bannerTts) bannerTts.textContent = `${cont1.time_to_spoilage_hours}h`;
    if (bannerBtn) {
      bannerBtn.disabled = false;
      bannerBtn.textContent = 'Authorize Pune Reroute';
      bannerBtn.className = 'btn btn-danger-glow';
    }
  } else {
    // Hidden or optimal
    banner.style.display = 'none';
  }
}

// ============================================================================
// API DISPATCH ACTIONS
// ============================================================================
// API DISPATCH ACTIONS & CLIENT-SIDE HYBRID SIMULATOR (VERCEL / RENDER READY)
// ============================================================================
let isOfflineMode = false;
let clientSimState = null;

function createClientSimulationState() {
  let cycle = 0;
  const reroutedSet = new Set();
  
  const MARKETS = {
    Nashik: { lat: 19.9975, lon: 73.7898, state: "Maharashtra" },
    Mumbai: { lat: 19.0760, lon: 72.8777, state: "Maharashtra" },
    Pune: { lat: 18.5204, lon: 73.8567, state: "Maharashtra" },
    Hyderabad: { lat: 17.3850, lon: 78.4867, state: "Telangana" },
    Bengaluru: { lat: 12.9716, lon: 77.5946, state: "Karnataka" },
    Chennai: { lat: 13.0827, lon: 80.2707, state: "Tamil Nadu" },
    Delhi: { lat: 28.7041, lon: 77.1025, state: "Delhi NCR" }
  };

  const history = {
    CONT_001: [],
    CONT_002: [],
    CONT_003: [],
    CONT_004: [],
    CONT_005: []
  };

  function stepInternal() {
    cycle++;
    const nowStr = new Date().toLocaleTimeString('en-US', { hour12: false });
    const temp001 = +(6.5 + Math.min(12.0, cycle * 1.5)).toFixed(2);
    
    const readings = [
      { cid: "CONT_001", ship: "SHIP_7894", comm: "Avocado", temp: temp001, hum: +(90.5 + (Math.random() * 3 - 1.5)).toFixed(1), vib: 0.18, lat: 19.3412, lon: 73.4521, origin: "Nashik", dest: "Mumbai", qty: 12000 },
      { cid: "CONT_002", ship: "SHIP_7895", comm: "Banana", temp: 14.2, hum: 91.0, vib: 0.12, lat: 18.7821, lon: 73.9102, origin: "Nashik", dest: "Pune", qty: 15000 },
      { cid: "CONT_003", ship: "SHIP_7896", comm: "Tomato", temp: 12.1, hum: 89.5, vib: 0.14, lat: 14.5200, lon: 77.6100, origin: "Hyderabad", dest: "Bengaluru", qty: 10000 },
      { cid: "CONT_004", ship: "SHIP_7897", comm: "Mango", temp: 13.5, hum: 88.0, vib: 0.11, lat: 23.4500, lon: 75.1200, origin: "Nashik", dest: "Delhi", qty: 8000 },
      { cid: "CONT_005", ship: "SHIP_7898", comm: "Apple", temp: 3.2, hum: 91.5, vib: 0.08, lat: 13.0100, lon: 79.1500, origin: "Bengaluru", dest: "Chennai", qty: 14000 }
    ];

    readings.forEach(r => {
      history[r.cid].push({
        step: cycle,
        temp: r.temp,
        hum: r.hum,
        vib: r.vib,
        time: nowStr,
        ...r
      });
      if (history[r.cid].length > 30) history[r.cid].shift();
    });
  }

  // Generate initial 3 cycles
  for (let i = 0; i < 3; i++) stepInternal();

  function buildPayload() {
    const fleet = [];
    const arbitrage = [];
    
    const specs = {
      Avocado: { opt: 6.0, crit: 12.0, max: 8.5, min: 4.5, maxH: 95, minH: 80, vibM: 0.25, shelf: 120, price: 185 },
      Banana: { opt: 14.0, crit: 18.0, max: 15.0, min: 13.0, maxH: 95, minH: 85, vibM: 0.20, shelf: 96, price: 48 },
      Tomato: { opt: 12.0, crit: 18.0, max: 14.0, min: 10.0, maxH: 92, minH: 85, vibM: 0.20, shelf: 144, price: 32 },
      Mango: { opt: 12.5, crit: 17.0, max: 14.0, min: 10.5, maxH: 95, minH: 85, vibM: 0.20, shelf: 168, price: 95 },
      Apple: { opt: 2.0, crit: 8.0, max: 4.0, min: 0.0, maxH: 96, minH: 88, vibM: 0.30, shelf: 720, price: 155 }
    };

    const cids = ["CONT_001", "CONT_002", "CONT_003", "CONT_004", "CONT_005"];
    cids.forEach(cid => {
      const events = history[cid];
      if (!events || events.length === 0) return;
      const latest = events[events.length - 1];
      const sp = specs[latest.comm] || specs.Tomato;
      const isRerouted = reroutedSet.has(cid);

      const recent = events.slice(-3);
      const avgTemp = +(recent.reduce((acc, e) => acc + e.temp, 0) / recent.length).toFixed(2);
      const avgHum = +(recent.reduce((acc, e) => acc + e.hum, 0) / recent.length).toFixed(2);

      const tempPenalty = Math.min(45.0, Math.max(0.0, ((avgTemp - sp.opt) / (sp.crit - sp.opt)) * 45.0));
      const humPenalty = Math.min(20.0, Math.max(0.0, (Math.abs(avgHum - 90.0) / 20.0) * 20.0));
      const vibPenalty = Math.min(15.0, Math.max(0.0, ((latest.vib - 0.10) / 0.35) * 15.0));
      const expPenalty = latest.temp > sp.max ? 12.0 : 0.0;

      const score = +Math.min(100.0, Math.max(0.0, tempPenalty + humPenalty + vibPenalty + expPenalty)).toFixed(1);
      let riskLevel = "Low";
      if (score >= 75.0) riskLevel = "Critical";
      else if (score >= 50.0) riskLevel = "High";
      else if (score >= 25.0) riskLevel = "Moderate";

      const decay = Math.max(0.05, 1.0 - (score / 100.0));
      const tts = +Math.max(1.0, sp.shelf * Math.pow(decay, 1.6)).toFixed(1);

      let action = "MAINTAIN_ROUTE";
      let dest = latest.dest;
      let recMarket = "N/A";
      let netArb = 0;
      let lossAvoided = 0;
      let potLoss = Math.round(latest.qty * sp.price * (score >= 50 ? 0.70 : 0.15));
      let divCost = 0;

      if (cid === "CONT_001") {
        recMarket = "Pune";
        divCost = 5575;
        lossAvoided = 630864;
        netArb = isRerouted ? 283189 : 283189;
        if (isRerouted) {
          action = "REROUTED";
          dest = "Pune (Rerouted)";
        } else if (riskLevel === "Critical" || riskLevel === "High") {
          action = "EXECUTE_REROUTE";
        }
      }

      fleet.push({
        container_id: cid,
        shipment_id: latest.ship,
        commodity: latest.comm,
        origin: latest.origin,
        destination: dest,
        quantity_kg: latest.qty,
        temperature_c: latest.temp,
        humidity_pct: latest.hum,
        vibration_g: latest.vib,
        latitude: latest.lat,
        longitude: latest.lon,
        rolling_avg_temp: avgTemp,
        rolling_avg_hum: avgHum,
        temp_penalty: +tempPenalty.toFixed(1),
        hum_penalty: +humPenalty.toFixed(1),
        vib_penalty: +vibPenalty.toFixed(1),
        exp_penalty: expPenalty,
        spoilage_risk_score: score,
        risk_level: riskLevel,
        time_to_spoilage_hours: tts,
        action: action,
        is_rerouted: isRerouted,
        min_safe_temp: sp.min,
        max_safe_temp: sp.max,
        min_safe_hum: sp.minH,
        max_safe_hum: sp.maxH
      });

      arbitrage.push({
        container_id: cid,
        commodity: latest.comm,
        quantity_kg: latest.qty,
        current_destination: isRerouted ? "Pune" : latest.dest,
        status_quo_value: latest.qty * sp.price - potLoss,
        potential_loss: potLoss,
        recommended_market: recMarket,
        recommended_price: 185.0,
        reroute_transit_hours: 2.8,
        reroute_expected_value: 2000000,
        diversion_cost: divCost,
        spoilage_loss_avoided: lossAvoided,
        net_arbitrage_inr: netArb,
        is_viable: true,
        directive: action
      });
    });

    const totalContainers = fleet.length;
    const atRisk = fleet.filter(c => c.risk_level === "High" || c.risk_level === "Critical").length;
    const critCount = fleet.filter(c => c.risk_level === "Critical").length;
    const avgT = +(fleet.reduce((a, b) => a + b.temperature_c, 0) / totalContainers).toFixed(1);
    const avgH = +(fleet.reduce((a, b) => a + b.humidity_pct, 0) / totalContainers).toFixed(1);
    const totArb = arbitrage.reduce((a, b) => a + (b.net_arbitrage_inr > 0 ? b.net_arbitrage_inr : 0), 0);
    const totAvoided = arbitrage.reduce((a, b) => a + (b.spoilage_loss_avoided > 0 ? b.spoilage_loss_avoided : 0), 0);

    return {
      cycle: cycle,
      kpis: {
        total_containers: totalContainers,
        at_risk_count: atRisk,
        critical_count: critCount,
        average_temp_c: avgT,
        average_humidity_pct: avgH,
        total_potential_arbitrage_inr: totArb,
        total_loss_avoided_inr: totAvoided
      },
      fleet: fleet,
      arbitrage: arbitrage,
      history: history,
      markets: MARKETS
    };
  }

  return {
    getData: () => buildPayload(),
    step: () => { stepInternal(); return buildPayload(); },
    reroute: (cid) => { reroutedSet.add(cid); return buildPayload(); },
    reset: () => {
      cycle = 0;
      reroutedSet.clear();
      Object.keys(history).forEach(k => history[k] = []);
      for (let i = 0; i < 3; i++) stepInternal();
      return buildPayload();
    }
  };
}

async function fetchConsoleData() {
  try {
    const res = await fetch('/api/data');
    if (!res.ok) throw new Error('API request failed');
    const data = await res.json();
    isOfflineMode = false;
    renderDashboard(data);
  } catch (err) {
    console.info('Backend API offline or static hosting detected; activating client simulator', err);
    isOfflineMode = true;
    if (!clientSimState) clientSimState = createClientSimulationState();
    renderDashboard(clientSimState.getData());
  }
}

async function simulateStep() {
  if (isOfflineMode) {
    if (!clientSimState) clientSimState = createClientSimulationState();
    renderDashboard(clientSimState.step());
    return;
  }
  try {
    const res = await fetch('/api/step', { method: 'POST' });
    if (!res.ok) throw new Error('Step request failed');
    const data = await res.json();
    renderDashboard(data);
  } catch (err) {
    console.warn('Switching to client simulation for step', err);
    isOfflineMode = true;
    if (!clientSimState) clientSimState = createClientSimulationState();
    renderDashboard(clientSimState.step());
  }
}

async function executeReroute(containerId) {
  if (isOfflineMode) {
    if (!clientSimState) clientSimState = createClientSimulationState();
    renderDashboard(clientSimState.reroute(containerId));
    switchTab('tab-live-console');
    return;
  }
  try {
    const res = await fetch('/api/reroute', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ container_id: containerId }),
    });
    if (!res.ok) throw new Error('Reroute request failed');
    const data = await res.json();
    renderDashboard(data);
    switchTab('tab-live-console');
  } catch (err) {
    console.warn('Switching to client simulation for reroute', err);
    isOfflineMode = true;
    if (!clientSimState) clientSimState = createClientSimulationState();
    renderDashboard(clientSimState.reroute(containerId));
    switchTab('tab-live-console');
  }
}

async function resetFleet() {
  if (isOfflineMode) {
    if (!clientSimState) clientSimState = createClientSimulationState();
    renderDashboard(clientSimState.reset());
    return;
  }
  try {
    const res = await fetch('/api/reset', { method: 'POST' });
    if (!res.ok) throw new Error('Reset request failed');
    const data = await res.json();
    renderDashboard(data);
  } catch (err) {
    console.warn('Switching to client simulation for reset', err);
    isOfflineMode = true;
    if (!clientSimState) clientSimState = createClientSimulationState();
    renderDashboard(clientSimState.reset());
  }
}


function toggleAutoStream() {
  const btn = document.getElementById('btn-stream');
  const text = document.getElementById('stream-text');

  if (autoStreamInterval) {
    clearInterval(autoStreamInterval);
    autoStreamInterval = null;
    text.textContent = 'Auto Stream';
    btn.classList.remove('btn-danger-glow');
    btn.classList.add('btn-primary');
  } else {
    autoStreamInterval = setInterval(simulateStep, 2500);
    text.textContent = 'Pause Stream';
    btn.classList.remove('btn-primary');
    btn.classList.add('btn-danger-glow');
  }
}

// ============================================================================
// EXECUTIVE BI VIEW (POWER BI / SUPERSET CLASS REPLICA ENGINE)
// ============================================================================
let pbiChartArbitrage = null;
let pbiChartDegradation = null;
let pbiChartMandi = null;

function initPbiCharts() {
  if (typeof Chart === 'undefined') return;

  const ctxArb = document.getElementById('pbiChartArbitrage');
  if (ctxArb && !pbiChartArbitrage) {
    pbiChartArbitrage = new Chart(ctxArb, {
      type: 'bar',
      data: {
        labels: ['CONT_001 (Avocado)', 'CONT_002 (Banana)', 'CONT_003 (Tomato)', 'CONT_004 (Mango)', 'CONT_005 (Apple)'],
        datasets: [
          {
            label: 'Scheduled Status-Quo Loss (₹)',
            data: [630864, 16470, 33720, 4710, 36736],
            backgroundColor: 'rgba(239, 68, 68, 0.85)',
            borderRadius: 4,
          },
          {
            label: 'Net Arbitrage Salvaged (₹)',
            data: [180826, 22120, 36370, 75360, 0],
            backgroundColor: 'rgba(16, 185, 129, 0.9)',
            borderRadius: 4,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { position: 'top', labels: { boxWidth: 10, font: { family: 'Outfit', size: 10 } } },
        },
        scales: {
          y: {
            ticks: {
              callback: (v) => '₹' + (v >= 1000 ? (v / 1000).toFixed(0) + 'k' : v),
              font: { family: 'JetBrains Mono', size: 9 },
            },
          },
          x: { ticks: { font: { family: 'Outfit', size: 9 } } },
        },
      },
    });
  }

  const ctxDeg = document.getElementById('pbiChartDegradation');
  if (ctxDeg && !pbiChartDegradation) {
    pbiChartDegradation = new Chart(ctxDeg, {
      type: 'line',
      data: {
        labels: ['0h', '2h', '4h', '6h', '8h', '10h', '12h'],
        datasets: [
          {
            label: 'CONT_001 Shelf Life (TTS Hours)',
            data: [120, 95, 68, 35, 20, 12, 5],
            borderColor: '#ef4444',
            backgroundColor: 'rgba(239, 68, 68, 0.15)',
            fill: true,
            tension: 0.35,
          },
          {
            label: 'Normal Baseline TTS',
            data: [120, 118, 116, 114, 112, 110, 108],
            borderColor: '#10b981',
            borderDash: [5, 5],
            tension: 0.1,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { position: 'top', labels: { boxWidth: 10, font: { family: 'Outfit', size: 10 } } },
        },
        scales: {
          y: { title: { display: true, text: 'TTS (Hours)', font: { size: 9 } }, ticks: { font: { size: 9 } } },
          x: { ticks: { font: { size: 9 } } },
        },
      },
    });
  }

  const ctxMandi = document.getElementById('pbiChartMandi');
  if (ctxMandi && !pbiChartMandi) {
    pbiChartMandi = new Chart(ctxMandi, {
      type: 'bar',
      data: {
        labels: ['Mumbai APMC', 'Pune APMC', 'Delhi Azadpur', 'Bengaluru Yeshwanthpur', 'Chennai Koyambedu'],
        datasets: [
          {
            label: 'Avocado (₹/kg)',
            data: [180, 185, 210, 175, 190],
            backgroundColor: 'rgba(56, 189, 248, 0.85)',
            borderRadius: 4,
          },
          {
            label: 'Mango (₹/kg)',
            data: [95, 90, 115, 85, 92],
            backgroundColor: 'rgba(245, 158, 11, 0.85)',
            borderRadius: 4,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { position: 'top', labels: { boxWidth: 10, font: { family: 'Outfit', size: 10 } } },
        },
        scales: {
          y: { ticks: { callback: (v) => '₹' + v, font: { size: 9 } } },
          x: { ticks: { font: { size: 9 } } },
        },
      },
    });
  }
}

function updatePbiTable(arbitrageList, fleetList) {
  const tbody = document.getElementById('pbi-table-body');
  if (!tbody) return;

  const list = arbitrageList && arbitrageList.length ? arbitrageList : [
    { container_id: 'CONT_001', commodity: 'Avocado', origin: 'Nashik', destination: 'Mumbai', risk_score: 82.4, tts: '20.0h', recommended: 'Pune', net_arb: 180826, directive: 'EXECUTE_REROUTE' },
    { container_id: 'CONT_002', commodity: 'Banana', origin: 'Nashik', destination: 'Pune', risk_score: 34.2, tts: '88.5h', recommended: 'Delhi', net_arb: 22120, directive: 'OPPORTUNITY_IDENTIFIED' },
    { container_id: 'CONT_003', commodity: 'Tomato', origin: 'Hyderabad', destination: 'Bengaluru', risk_score: 41.5, tts: '96.2h', recommended: 'Delhi', net_arb: 36370, directive: 'OPPORTUNITY_IDENTIFIED' },
    { container_id: 'CONT_004', commodity: 'Mango', origin: 'Bengaluru', destination: 'Chennai', risk_score: 22.0, tts: '145.0h', recommended: 'Delhi', net_arb: 75360, directive: 'OPPORTUNITY_IDENTIFIED' },
    { container_id: 'CONT_005', commodity: 'Apple', origin: 'Shimla', destination: 'Delhi', risk_score: 8.5, tts: '680.0h', recommended: 'Chennai', net_arb: -82614, directive: 'MAINTAIN_ROUTE' },
  ];

  tbody.innerHTML = list.map((item) => {
    const isReroute = item.directive === 'EXECUTE_REROUTE';
    const isOpp = item.directive === 'OPPORTUNITY_IDENTIFIED';
    const badgeColor = isReroute ? '#ef4444' : isOpp ? '#0284c7' : '#10b981';
    const badgeBg = isReroute ? 'rgba(239, 68, 68, 0.15)' : isOpp ? 'rgba(2, 132, 199, 0.15)' : 'rgba(16, 185, 129, 0.15)';
    const netArbVal = item.net_arb !== undefined ? item.net_arb : item.net_arbitrage_inr;
    const scoreVal = item.risk_score !== undefined ? item.risk_score : item.spoilage_risk_score;
    const ttsVal = item.tts || (item.time_to_spoilage_hours ? item.time_to_spoilage_hours + 'h' : '45.0h');
    const destVal = item.destination || item.current_destination || 'Mumbai';
    const recVal = item.recommended || item.recommended_market || 'Pune';

    return `
      <tr style="border-bottom: 1px solid #e2e8f0;">
        <td style="padding: 6px 8px; font-weight: 700;">${item.container_id}</td>
        <td style="padding: 6px 8px;">${item.commodity}</td>
        <td style="padding: 6px 8px; color: #64748b;">${item.origin || 'Nashik'} &rarr; ${destVal}</td>
        <td style="padding: 6px 8px; font-weight: 700; color: ${scoreVal >= 70 ? '#ef4444' : scoreVal >= 40 ? '#f59e0b' : '#10b981'};">${scoreVal}</td>
        <td style="padding: 6px 8px; font-family: var(--font-mono);">${ttsVal}</td>
        <td style="padding: 6px 8px; font-weight: 600; color: #0284c7;">${recVal}</td>
        <td style="padding: 6px 8px; font-family: var(--font-mono); font-weight: 700; color: ${netArbVal > 0 ? '#10b981' : '#64748b'};">${formatINR(netArbVal)}</td>
        <td style="padding: 6px 8px;">
          <span style="font-size: 0.68rem; font-weight: 700; padding: 2px 6px; border-radius: 4px; background: ${badgeBg}; color: ${badgeColor}; border: 1px solid ${badgeColor};">
            ${item.directive}
          </span>
        </td>
      </tr>
    `;
  }).join('');
}

function filterPbiData() {
  const commFilter = document.getElementById('pbi-filter-commodity')?.value || 'ALL';
  const riskFilter = document.getElementById('pbi-filter-risk')?.value || 'ALL';

  let filtered = currentArbitrageData && currentArbitrageData.length ? [...currentArbitrageData] : [];
  if (commFilter !== 'ALL') {
    filtered = filtered.filter((f) => f.commodity === commFilter);
  }
  if (riskFilter !== 'ALL') {
    filtered = filtered.filter((f) => {
      const score = f.spoilage_risk_score || 0;
      if (riskFilter === 'Critical') return score >= 75;
      if (riskFilter === 'High') return score >= 50 && score < 75;
      if (riskFilter === 'Moderate') return score >= 25 && score < 50;
      return score < 25;
    });
  }
  updatePbiTable(filtered, currentFleetData);
}

function switchPbiSheet(sheetKey) {
  document.querySelectorAll('.pbi-sheet-tab').forEach((b) => b.classList.remove('active'));
  const activeTab = document.getElementById(`pbi-tab-${sheetKey}`);
  if (activeTab) activeTab.classList.add('active');

  const bannerTitle = document.querySelector('.pbi-title-group h2');
  if (bannerTitle) {
    if (sheetKey === 'exec') bannerTitle.textContent = 'COLD-CHAIN ARBITRAGE DECISION TOWER (EXECUTIVE SUMMARY)';
    else if (sheetKey === 'environ') bannerTitle.textContent = 'ENVIRONMENTAL TELEMATICS & KINETICS BREAKDOWN';
    else if (sheetKey === 'logistics') bannerTitle.textContent = 'LOGISTICS FREIGHT CORRIDORS & WHOLESALE SPREADS';
    else if (sheetKey === 'marts') bannerTitle.textContent = 'SNOWFLAKE ANALYTICS MARTS (FCT_SPOILAGE_ARBITRAGE)';
  }
}

function renderDashboard(data) {
  if (!data) return;

  const cycleBadge = document.getElementById('cycle-badge');
  if (cycleBadge) cycleBadge.textContent = `Cycle #${data.cycle || 1}`;

  currentFleetData = data.fleet || [];
  currentArbitrageData = data.arbitrage || [];

  updateKPIs(data.kpis);
  updateIncidentBanner(data.fleet, data.arbitrage);
  updateMap(data.fleet, data.markets);
  updateLeaderboard(data.fleet);
  updateCharts(data.history);
  updateArbitrageTable(data.arbitrage, data.fleet);

  // Update Executive BI replica
  updatePbiTable(data.arbitrage, data.fleet);
  if (data.kpis) {
    const kpiFleet = document.getElementById('pbi-kpi-fleet');
    if (kpiFleet) kpiFleet.textContent = data.kpis.total_containers || 5;
    const kpiCrit = document.getElementById('pbi-kpi-critical');
    if (kpiCrit) kpiCrit.textContent = data.kpis.critical_containers || 1;
    const kpiTemp = document.getElementById('pbi-kpi-temp');
    if (kpiTemp) kpiTemp.textContent = `${data.kpis.average_temperature_c || 11.8} °C`;
  }
}

// ============================================================================
// INITIALIZATION ON DOM READY
// ============================================================================
document.addEventListener('DOMContentLoaded', () => {
  // 1. Initialize Tabs & Slide Presentation Router
  initTabRouter();

  // 2. Initialize Layer 1 Detail Card by default
  selectLayer('layer1');

  // 3. Initialize Math Sandbox with Avocado preset
  updateMathSandbox();

  // 3b. Attach explicit input & change listeners for all 5 math lab sliders
  ['slider-temp', 'slider-hum', 'slider-vib', 'slider-alerts', 'slider-dist'].forEach((id) => {
    const el = document.getElementById(id);
    if (el) {
      el.addEventListener('input', updateMathSandbox);
      el.addEventListener('change', updateMathSandbox);
    }
  });

  // 4. Initialize Map & Charts
  initMap();
  initCharts();
  initPbiCharts();
  updatePbiTable(currentArbitrageData, currentFleetData);

  // 5. Connect Global Header Buttons
  document.getElementById('btn-step')?.addEventListener('click', simulateStep);
  document.getElementById('btn-stream')?.addEventListener('click', toggleAutoStream);
  document.getElementById('btn-reset')?.addEventListener('click', resetFleet);

  // 6. Connect Table Search Filter
  document.getElementById('table-search')?.addEventListener('input', () => {
    updateArbitrageTable(currentArbitrageData, currentFleetData);
  });

  // 7. Initial Data Fetch
  fetchConsoleData();
});

