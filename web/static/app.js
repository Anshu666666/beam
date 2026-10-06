// ==========================================================================
// Beam — High-Throughput Concurrent Byte-Range Streaming Engine (app.js)
// Anime.js Motion Paths, Windows XP Byte Packet Streaming, 2D Completion Grid
// ==========================================================================

document.addEventListener('DOMContentLoaded', () => {
  console.log('[beam] DOM fully loaded. Initializing Beam engine...');

  // 1. Elements
  const themeLightBtn = document.getElementById('theme-light-btn');
  const themeDarkBtn = document.getElementById('theme-dark-btn');

  const urlInput = document.getElementById('url-input');
  const workersSlider = document.getElementById('workers-slider');
  const workersVal = document.getElementById('workers-val');
  const chunksSlider = document.getElementById('chunks-slider');
  const chunksVal = document.getElementById('chunks-val');
  const startBtn = document.getElementById('start-btn');
  const presetBtns = document.querySelectorAll('.preset-pill');

  // Telemetry
  const stageStatusBadge = document.getElementById('stage-status-badge');
  const stageFilename = document.getElementById('stage-filename');
  const masterPercent = document.getElementById('master-percent');
  const masterBytes = document.getElementById('master-bytes');
  const masterSpeed = document.getElementById('master-speed');
  const masterEta = document.getElementById('master-eta');
  const masterFill = document.getElementById('master-fill');

  // Destination Folder & Arena
  const destFolder = document.getElementById('dest-folder');
  const folderFilenameDisplay = document.getElementById('folder-filename-display');
  const folderBytesDisplay = document.getElementById('folder-bytes-display');
  const destFolderTarget = document.getElementById('dest-folder-target');
  const verificationSeal = document.getElementById('verification-seal');
  const packetsIngestedCount = document.getElementById('packets-ingested-count');
  const integrityStatus = document.getElementById('integrity-status');

  // Conduits & Workers Rack
  const conduitsContainer = document.getElementById('conduits-container');
  const conduitsSvg = document.getElementById('conduits-svg');
  const bytePacketsPlane = document.getElementById('byte-packets-plane');
  const workerEntitiesRack = document.getElementById('worker-entities-rack');
  const workersCountBadge = document.getElementById('workers-count-badge');

  // 2D Chunk Completion Matrix
  const matrixGrid = document.getElementById('matrix-grid-viewport');
  const matrixCompletedCount = document.getElementById('matrix-completed-count');
  const matrixCoveragePct = document.getElementById('matrix-coverage-pct');

  // Trace Window
  const traceWindow = document.getElementById('trace-window');
  const clearTraceBtn = document.getElementById('clear-trace-btn');

  // Internal Engine State
  let activeEventSource = null;
  let isDownloading = false;
  let totalPacketsIngested = 0;
  let packetSpawnerInterval = null;
  let completedChunksSet = new Set();
  let currentTotalChunks = 16;

  // 2. Safe Conduit Calculation & Rendering
  function renderConduits(count) {
    if (!conduitsSvg || !conduitsContainer || !destFolderTarget) return;

    conduitsSvg.innerHTML = '';
    const containerRect = conduitsContainer.getBoundingClientRect();
    const targetRect = destFolderTarget.getBoundingClientRect();

    if (containerRect.width === 0) {
      setTimeout(() => renderConduits(count), 100);
      return;
    }

    const x2 = targetRect.left - containerRect.left + targetRect.width / 2;
    const y2 = targetRect.top - containerRect.top + targetRect.height / 2;

    for (let i = 0; i < count; i++) {
      const emitterEl = document.getElementById(`worker-emitter-${i}`);
      if (!emitterEl) continue;

      const emitterRect = emitterEl.getBoundingClientRect();
      const x1 = emitterRect.left - containerRect.left + emitterRect.width / 2;
      const y1 = emitterRect.top - containerRect.top + emitterRect.height / 2;

      const dx = x1 - x2;
      const cp1x = x1 - dx * 0.55;
      const cp1y = y1;
      const cp2x = x2 + dx * 0.45;
      const cp2y = y2;

      const pathData = `M ${x1} ${y1} C ${cp1x} ${cp1y}, ${cp2x} ${cp2y}, ${x2} ${y2}`;

      const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      path.setAttribute('id', `conduit-path-${i}`);
      path.setAttribute('class', 'conduit-track');
      path.setAttribute('d', pathData);
      conduitsSvg.appendChild(path);
    }
  }

  // 3. Theme Switcher (Default: Light Mode)
  function setTheme(theme) {
    console.log('[beam] Theme switched to:', theme);
    document.documentElement.setAttribute('data-theme', theme);
    try { localStorage.setItem('beam-theme', theme); } catch (e) {}

    if (themeLightBtn && themeDarkBtn) {
      if (theme === 'dark') {
        themeDarkBtn.classList.add('active');
        themeLightBtn.classList.remove('active');
      } else {
        themeLightBtn.classList.add('active');
        themeDarkBtn.classList.remove('active');
      }
    }

    if (workersSlider) {
      const count = parseInt(workersSlider.value, 10);
      renderConduits(count);
    }
  }

  if (themeLightBtn) themeLightBtn.addEventListener('click', () => setTheme('light'));
  if (themeDarkBtn) themeDarkBtn.addEventListener('click', () => setTheme('dark'));

  let savedTheme = 'light';
  try { savedTheme = localStorage.getItem('beam-theme') || 'light'; } catch (e) {}
  setTheme(savedTheme);

  // 4. 2D Visual Completion Grid Engine (BitTorrent / Defrag Style)
  function initChunkMatrix(chunkCount) {
    if (!matrixGrid) return;
    currentTotalChunks = chunkCount;
    matrixGrid.innerHTML = '';
    completedChunksSet.clear();

    if (matrixCompletedCount) {
      matrixCompletedCount.textContent = `0 / ${chunkCount} Chunks Stitched`;
    }
    if (matrixCoveragePct) {
      matrixCoveragePct.textContent = '0.0% Coverage';
    }

    for (let i = 0; i < chunkCount; i++) {
      const cell = document.createElement('div');
      cell.className = 'chunk-cell';
      cell.id = `chunk-cell-${i}`;
      cell.textContent = i;
      cell.title = `Chunk #${i}: Pending partition`;
      matrixGrid.appendChild(cell);
    }
  }

  function updateChunkMatrix(workers, isFinished = false) {
    if (!matrixGrid) return;

    if (isFinished) {
      for (let i = 0; i < currentTotalChunks; i++) {
        completedChunksSet.add(i);
        const cell = document.getElementById(`chunk-cell-${i}`);
        if (cell) {
          cell.className = 'chunk-cell status-completed';
          cell.title = `Chunk #${i}: 100% Stitched & Verified`;
        }
      }
      if (matrixCompletedCount) {
        matrixCompletedCount.textContent = `${currentTotalChunks} / ${currentTotalChunks} Chunks Stitched`;
      }
      if (matrixCoveragePct) {
        matrixCoveragePct.textContent = '100.0% Coverage';
      }

      if (typeof anime !== 'undefined') {
        anime({
          targets: '.chunk-cell',
          scale: [1, 1.25, 1],
          delay: anime.stagger(15),
          duration: 350,
          easing: 'spring(1, 80, 10, 0)'
        });
      }
      return;
    }

    if (workers && Array.isArray(workers)) {
      workers.forEach((w) => {
        if (w.currentChunk !== undefined && w.currentChunk < currentTotalChunks) {
          const chunkId = w.currentChunk;
          const cell = document.getElementById(`chunk-cell-${chunkId}`);
          if (!cell) return;

          if (w.percent >= 100 || w.status === 'done') {
            completedChunksSet.add(chunkId);
            cell.className = 'chunk-cell status-completed';
            cell.title = `Chunk #${chunkId}: Completed (${w.totalFormatted || ''})`;
          } else if (w.chunkDownloaded > 0) {
            if (!completedChunksSet.has(chunkId)) {
              cell.className = 'chunk-cell status-downloading';
              cell.title = `Chunk #${chunkId}: ${w.percent.toFixed(0)}% (Worker #${w.id})`;
            }
          }
        }
      });
    }

    const count = completedChunksSet.size;
    const coverage = ((count / currentTotalChunks) * 100).toFixed(1);
    if (matrixCompletedCount) {
      matrixCompletedCount.textContent = `${count} / ${currentTotalChunks} Chunks Stitched`;
    }
    if (matrixCoveragePct) {
      matrixCoveragePct.textContent = `${coverage}% Coverage`;
    }
  }

  // 5. Worker Rack Initialization
  function initWorkerRack(count) {
    if (!workerEntitiesRack) return;
    workerEntitiesRack.innerHTML = '';

    for (let i = 0; i < count; i++) {
      const node = document.createElement('div');
      node.className = 'worker-node';
      node.id = `worker-node-${i}`;
      node.innerHTML = `
        <div class="worker-emitter-port" id="worker-emitter-${i}" title="Byte packet dispatch port"></div>
        <div class="worker-node-top">
          <div class="worker-id-group">
            <span class="worker-led" id="worker-led-${i}"></span>
            <span class="worker-name-label">Worker #${i}</span>
          </div>
          <span class="worker-status-badge" id="worker-status-${i}">IDLE</span>
        </div>
        <div class="worker-chunk-tag" id="worker-chunk-${i}">Standing by for byte interval...</div>
        <div class="worker-mini-track">
          <div class="worker-mini-fill" id="worker-fill-${i}"></div>
        </div>
        <div class="worker-node-bottom">
          <span id="worker-bytes-${i}">0 B</span>
          <span id="worker-pct-${i}">0.0%</span>
        </div>
      `;
      workerEntitiesRack.appendChild(node);
    }
  }

  // 6. Slider Handlers
  if (workersSlider) {
    workersSlider.addEventListener('input', (e) => {
      const val = parseInt(e.target.value, 10);
      if (workersVal) workersVal.textContent = val;
      console.log('[beam] Workers configured:', val);
      if (workersCountBadge) workersCountBadge.textContent = `${val} Workers Active`;
      if (!isDownloading) {
        initWorkerRack(val);
        setTimeout(() => renderConduits(val), 50);
      }
    });
  }

  if (chunksSlider) {
    chunksSlider.addEventListener('input', (e) => {
      const val = parseInt(e.target.value, 10);
      if (chunksVal) chunksVal.textContent = val;
      console.log('[beam] Chunks matrix configured:', val);
      if (!isDownloading) {
        initChunkMatrix(val);
      }
    });
  }

  // 7. Demonstration Preset Selectors
  presetBtns.forEach((btn) => {
    btn.addEventListener('click', () => {
      presetBtns.forEach((b) => b.classList.remove('active'));
      btn.classList.add('active');

      let resolvedUrl = btn.dataset.url;
      if (resolvedUrl && resolvedUrl.startsWith('/')) {
        resolvedUrl = window.location.origin + resolvedUrl;
      }
      if (urlInput) urlInput.value = resolvedUrl;

      if (btn.dataset.workers && workersSlider) {
        workersSlider.value = btn.dataset.workers;
        if (workersVal) workersVal.textContent = btn.dataset.workers;
        if (workersCountBadge) workersCountBadge.textContent = `${btn.dataset.workers} Workers Active`;
      }
      if (btn.dataset.chunks && chunksSlider) {
        chunksSlider.value = btn.dataset.chunks;
        if (chunksVal) chunksVal.textContent = btn.dataset.chunks;
      }

      const wCount = parseInt(workersSlider.value, 10);
      const cCount = parseInt(chunksSlider.value, 10);
      console.log('[beam] Preset button clicked:', label, '->', btn.dataset.url);

      initWorkerRack(wCount);
      initChunkMatrix(cCount);
      setTimeout(() => renderConduits(wCount), 50);

      const label = btn.querySelector('.preset-pill-tag') ? btn.querySelector('.preset-pill-tag').textContent : 'Preset';
      appendTrace(`[preset] Selected ${label} -> ${btn.dataset.url} (Chunks: ${cCount})`, 'log-probe');
    });
  });

  // 8. Diagnostic Protocol Trace Window
  if (clearTraceBtn) {
    clearTraceBtn.addEventListener('click', () => {
      console.log('[beam] Diagnostic trace cleared.');
      if (traceWindow) {
        traceWindow.innerHTML = '<div class="trace-log-line log-system">[00:00:00] [system] Protocol trace buffer cleared.</div>';
      }
    });
  }

  function appendTrace(text, className = 'log-system') {
    if (!traceWindow) return;
    const line = document.createElement('div');
    line.className = `trace-log-line ${className}`;
    const timestamp = new Date().toLocaleTimeString();
    line.textContent = `[${timestamp}] ${text}`;
    traceWindow.appendChild(line);
    traceWindow.scrollTop = traceWindow.scrollHeight;
  }

  // 9. Anime.js Byte Packet Flight Animation Engine
  const packetIcons = ['01', '10', '📦', '📄', '00', '11'];

  function spawnBytePacket(workerId) {
    const pathSelector = `#conduit-path-${workerId}`;
    const pathEl = document.querySelector(pathSelector);
    if (!pathEl || typeof anime === 'undefined' || !bytePacketsPlane) return;

    try {
      const pathFunc = anime.path(pathSelector);
      const packet = document.createElement('div');
      const isData = Math.random() > 0.4;
      packet.className = `byte-packet ${isData ? 'type-data' : 'type-chunk'}`;
      packet.textContent = packetIcons[Math.floor(Math.random() * packetIcons.length)];
      bytePacketsPlane.appendChild(packet);

      const flightDuration = 650 + Math.random() * 350;

      anime({
        targets: packet,
        translateX: pathFunc('x'),
        translateY: pathFunc('y'),
        rotate: pathFunc('angle'),
        easing: 'cubicBezier(0.25, 0.1, 0.25, 1.0)',
        duration: flightDuration,
        complete: () => {
          packet.remove();
          triggerFolderIngestionBounce();
        }
      });
    } catch (err) {
      console.warn('Anime.js path motion error:', err);
    }
  }

  function triggerFolderIngestionBounce() {
    totalPacketsIngested++;
    if (packetsIngestedCount) {
      packetsIngestedCount.textContent = totalPacketsIngested.toLocaleString();
    }

    if (destFolder && typeof anime !== 'undefined') {
      anime({
        targets: destFolder,
        scale: [1, 1.04, 0.98, 1],
        rotate: [0, -1, 1, 0],
        duration: 160,
        easing: 'easeInOutQuad'
      });
    }
  }

  function startPacketStreamingEngine(workerCount) {
    if (packetSpawnerInterval) clearInterval(packetSpawnerInterval);

    packetSpawnerInterval = setInterval(() => {
      if (!isDownloading) return;

      for (let i = 0; i < workerCount; i++) {
        const workerNode = document.getElementById(`worker-node-${i}`);
        if (workerNode && workerNode.classList.contains('active-streaming')) {
          if (Math.random() > 0.2) {
            spawnBytePacket(i);
          }
        }
      }
    }, 120);
  }

  function stopPacketStreamingEngine() {
    if (packetSpawnerInterval) {
      clearInterval(packetSpawnerInterval);
      packetSpawnerInterval = null;
    }
  }

  // 10. Start Concurrent Download (SSE Stream Connection)
  if (startBtn) {
    startBtn.addEventListener('click', () => {
      console.log('[beam] START button clicked!');
      if (isDownloading) return;

      let rawUrl = urlInput ? urlInput.value.trim() : '';
      if (!rawUrl) {
        alert('Please enter a target URL.');
        return;
      }
      const url = rawUrl.startsWith('/') ? (window.location.origin + rawUrl) : rawUrl;
      if (!url) {
        alert('Please enter a target URL.');
        return;
      }

      const workers = workersSlider ? parseInt(workersSlider.value, 10) : 4;
      const chunks = chunksSlider ? parseInt(chunksSlider.value, 10) : 16;

      isDownloading = true;
      startBtn.disabled = true;
      totalPacketsIngested = 0;
      if (packetsIngestedCount) packetsIngestedCount.textContent = '0';
      if (integrityStatus) integrityStatus.textContent = 'Streaming...';
      if (verificationSeal) verificationSeal.classList.remove('revealed');
      if (masterFill) masterFill.style.width = '0%';
      if (masterPercent) masterPercent.innerHTML = '0.0<small>%</small>';
      if (masterSpeed) masterSpeed.textContent = '0 B/s';
      if (masterEta) masterEta.textContent = '--';

      if (stageStatusBadge) {
        stageStatusBadge.textContent = 'STREAMING';
        stageStatusBadge.className = 'badge-status-pill streaming';
      }

      initWorkerRack(workers);
      initChunkMatrix(chunks);
      renderConduits(workers);
      startPacketStreamingEngine(workers);

      appendTrace(`[beam] Initiating stream: ${url} (Workers: ${workers}, Chunks: ${chunks})`, 'log-system');

      const sseUrl = `/api/download/stream?url=${encodeURIComponent(url)}&workers=${workers}&chunks=${chunks}`;
      if (activeEventSource) {
        activeEventSource.close();
      }

      activeEventSource = new EventSource(sseUrl);

      activeEventSource.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          if (data.type === 'start' || data.type === 'complete' || data.type === 'error') {
            console.log('[beam] SSE Event [' + data.type + ']:', data);
          }
          handleStreamEvent(data, workers);
        } catch (err) {
          console.error('Failed to parse SSE event JSON:', err);
        }
      };

      activeEventSource.onerror = (err) => {
        console.warn('SSE stream closed or interrupted:', err);
        finishDownload(false, 'Stream disconnected');
      };
    });
  }

  // 11. Process Stream Events
  function handleStreamEvent(data, workerCount) {
    switch (data.type) {
      case 'start':
        if (stageFilename) stageFilename.textContent = data.filename || 'unknown';
        if (folderFilenameDisplay) folderFilenameDisplay.textContent = data.filename || 'destination.bin';
        appendTrace(`Target verified: ${data.filename} (${data.master?.totalFormatted || 'size pending'})`, 'log-probe');
        if (data.rangeSupported) {
          appendTrace('RFC 7233 Accept-Ranges: bytes confirmed. Slicing into concurrent intervals.', 'log-range');
        } else {
          appendTrace('Range NOT supported by server. Falling back to single-stream mode.', 'log-range');
        }
        break;

      case 'progress':
        if (data.master) {
          const m = data.master;
          if (masterPercent) masterPercent.innerHTML = `${m.percent.toFixed(1)}<small>%</small>`;
          if (masterFill) masterFill.style.width = `${m.percent}%`;
          if (masterBytes) masterBytes.textContent = `${m.downloadedFormatted} / ${m.totalFormatted}`;
          if (masterSpeed) masterSpeed.textContent = m.speedFormatted || '0 B/s';
          if (folderBytesDisplay) folderBytesDisplay.textContent = `${m.downloadedFormatted} absorbed`;

          if (masterEta) {
            if (m.etaSeconds > 0) {
              masterEta.textContent = `${m.etaSeconds}s`;
            } else {
              masterEta.textContent = m.percent >= 100 ? '0s' : '--';
            }
          }
        }

        updateChunkMatrix(data.workers, false);

        if (data.workers && Array.isArray(data.workers)) {
          data.workers.forEach((w) => {
            const node = document.getElementById(`worker-node-${w.id}`);
            const statusBadge = document.getElementById(`worker-status-${w.id}`);
            const led = document.getElementById(`worker-led-${w.id}`);
            const chunkTag = document.getElementById(`worker-chunk-${w.id}`);
            const fill = document.getElementById(`worker-fill-${w.id}`);
            const bytesLabel = document.getElementById(`worker-bytes-${w.id}`);
            const pctLabel = document.getElementById(`worker-pct-${w.id}`);
            const conduitPath = document.getElementById(`conduit-path-${w.id}`);

            if (!node) return;

            const isDone = w.status === 'idle' || w.percent >= 100;
            const isStreaming = w.status === 'downloading' || (!isDone && w.chunkDownloaded > 0);

            if (isStreaming) {
              node.className = 'worker-node active-streaming';
              if (statusBadge) {
                statusBadge.textContent = 'PULLING';
                statusBadge.className = 'worker-status-badge streaming';
              }
              if (led) led.className = 'worker-led led-active';
              if (conduitPath) conduitPath.className.baseVal = 'conduit-track active';
            } else if (isDone && w.chunkDownloaded > 0) {
              node.className = 'worker-node worker-complete';
              if (statusBadge) {
                statusBadge.textContent = 'DONE';
                statusBadge.className = 'worker-status-badge completed';
              }
              if (led) led.className = 'worker-led led-done';
              if (conduitPath) conduitPath.className.baseVal = 'conduit-track complete';
            } else {
              node.className = 'worker-node';
              if (statusBadge) {
                statusBadge.textContent = 'IDLE';
                statusBadge.className = 'worker-status-badge';
              }
              if (led) led.className = 'worker-led';
              if (conduitPath) conduitPath.className.baseVal = 'conduit-track';
            }

            if (chunkTag && w.currentChunk !== undefined && w.chunkSize) {
              chunkTag.textContent = `Chunk #${w.currentChunk} (${w.totalFormatted})`;
            }

            if (fill) fill.style.width = `${w.percent}%`;
            if (bytesLabel) bytesLabel.textContent = w.downloadedFormatted || '0 B';
            if (pctLabel) pctLabel.textContent = `${w.percent.toFixed(0)}%`;
          });
        }
        break;

      case 'complete':
        appendTrace(`[beam] Ingestion complete! Duration: ${data.duration}. Average speed: ${data.averageSpeed}`, 'log-success');
        updateChunkMatrix(data.workers, true);
        finishDownload(true);
        break;

      case 'error':
        const errDesc = data.error || data.message || 'Stream connection error';
        appendTrace(`[beam] Stream error: ${errDesc}`, 'log-error');
        finishDownload(false, errDesc);
        break;
    }
  }

  // 12. Teardown & Completion Handling
  function finishDownload(success, errMsg = '') {
    isDownloading = false;
    if (startBtn) startBtn.disabled = false;
    stopPacketStreamingEngine();

    if (activeEventSource) {
      activeEventSource.close();
      activeEventSource = null;
    }

    if (success) {
      if (stageStatusBadge) {
        stageStatusBadge.textContent = 'COMPLETE';
        stageStatusBadge.className = 'badge-status-pill';
        stageStatusBadge.style.backgroundColor = 'var(--color-butter)';
      }
      if (integrityStatus) integrityStatus.textContent = 'SHA-256 Verified';

      if (masterFill) masterFill.style.width = '100%';
      if (masterPercent) masterPercent.innerHTML = '100.0<small>%</small>';
      if (masterEta) masterEta.textContent = '0s';

      if (verificationSeal) verificationSeal.classList.add('revealed');

      if (destFolder && typeof anime !== 'undefined') {
        anime({
          targets: destFolder,
          scale: [1, 1.1, 1],
          duration: 400,
          easing: 'spring(1, 80, 10, 0)'
        });
      }
    } else {
      if (stageStatusBadge) {
        stageStatusBadge.textContent = 'ABORTED';
        stageStatusBadge.className = 'badge-status-pill';
        stageStatusBadge.style.backgroundColor = 'var(--color-coral)';
      }
      if (integrityStatus) integrityStatus.textContent = errMsg || 'Failed';
    }
  }

  window.addEventListener('resize', () => {
    if (workersSlider) {
      const count = parseInt(workersSlider.value, 10);
      renderConduits(count);
    }
  });

  if (urlInput && urlInput.value && urlInput.value.startsWith('/')) {
    urlInput.value = window.location.origin + urlInput.value;
  }
  const initialWorkers = workersSlider ? parseInt(workersSlider.value, 10) : 4;
  const initialChunks = chunksSlider ? parseInt(chunksSlider.value, 10) : 16;

  initWorkerRack(initialWorkers);
  initChunkMatrix(initialChunks);
  setTimeout(() => renderConduits(initialWorkers), 100);

  console.log('[beam] Beam Concurrency Engine UI initialized successfully with 2D Chunk Matrix.');
});