// Update WebSocket connection to use our API endpoint
const socket = io(window.location.origin);
const charts = {};
const maxDataPoints = 50;

// Add trading signals storage for each stock
const tradingSignals = {
    AAPL: [], GOOGL: [], AMZN: [], MSFT: [], TSLA: []
};

// Add function to fetch initial data from API
async function loadInitialData() {
    try {
        const [liveData, tradingSignals] = await Promise.all([
            fetch('/api/live-data').then(res => res.json()),
            fetch('/api/trading-signals').then(res => res.json())
        ]);

        // Initialize charts with historical data
        initializeChartsWithData(liveData);
        
        // Add trading signals
        tradingSignals.forEach(signal => {
            addSignalToContainer(signal);
            addSignalToChart(signal);
        });

        console.log('Initial data loaded successfully');
    } catch (error) {
        console.error('Error loading initial data:', error);
        showNotification('Error loading initial data', 'error');
    }
}

function getSignalConfig(type) {
    const configs = {
        'BUY': {
            color: '#22c55e',
            className: 'signal-point-buy',
            symbol: '↑',
            text: 'BUY'
        },
        'SELL': {
            color: '#ef4444',
            className: 'signal-point-sell',
            symbol: '↓',
            text: 'SELL'
        },
        'HOLD': {
            color: '#eab308',
            className: 'signal-point-hold',
            symbol: '•',
            text: 'HOLD'
        }
    };
    return configs[type?.toUpperCase()] || configs['HOLD'];
}

function initializeChartsWithData(initialData) {
    const stockData = {};
    
    // Group data by stock symbol
    initialData.forEach(data => {
        if (!stockData[data.stock_symbol]) {
            stockData[data.stock_symbol] = [];
        }
        stockData[data.stock_symbol].push(data);
    });

    // Initialize each chart with its data
    Object.entries(stockData).forEach(([symbol, data]) => {
        initializeChart(symbol, data);
    });
}

function initializeChart(symbol, data) {
    const canvas = document.getElementById(`chart${symbol}`);
    if (!canvas) {
        console.error(`Canvas not found for ${symbol}`);
        return;
    }

    // Clear existing chart if it exists
    if (charts[symbol]) {
        charts[symbol].destroy();
    }

    charts[symbol] = new Chart(canvas.getContext('2d'), {
        type: 'line',
        data: {
            labels: data.map(d => new Date(d.timestamp).toLocaleTimeString()),
            datasets: [{
                label: `${symbol} Price`,
                data: data.map(d => d.current_price),
                borderColor: getStockColor(symbol),
                borderWidth: 2,
                fill: false,
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)',
                        drawBorder: false
                    },
                    ticks: {
                        color: '#a0a0a0',
                        callback: function(value) {
                            return '$' + value.toFixed(2);
                        }
                    }
                },
                x: {
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)',
                        drawBorder: false
                    },
                    ticks: { color: '#a0a0a0' }
                }
            },
            plugins: {
                annotation: {
                    annotations: {}
                },
                title: {
                    display: true,
                    text: `${symbol} Stock Price`
                },
                legend: {
                    display: false
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    backgroundColor: 'rgba(26, 26, 26, 0.9)',
                    titleColor: '#e0e0e0',
                    bodyColor: '#e0e0e0',
                    borderColor: '#2d2d2d',
                    borderWidth: 1
                }
            },
            interaction: {
                mode: 'nearest',
                axis: 'x',
                intersect: false
            }
        }
    });
}

function addSignalToChart(signal) {
    const chart = charts[signal.stock];
    if (!chart) return;

    const signalConfig = getSignalConfig(signal.signal);
    const index = chart.data.labels.length - 1;

    const annotation = {
        type: 'point',
        xValue: index,
        yValue: signal.price,
        backgroundColor: signalConfig.color,
        borderColor: 'white',
        borderWidth: 2,
        radius: 6,
        label: {
            content: signalConfig.symbol,
            enabled: true,
            position: 'top'
        }
    };

    if (!chart.options.plugins.annotation.annotations) {
        chart.options.plugins.annotation.annotations = {};
    }
    
    const annotationKey = `signal-${Date.now()}`;
    chart.options.plugins.annotation.annotations[annotationKey] = annotation;
    chart.update('none');
}

function updateChart(data) {
    if (!data || !data.stock_symbol || !charts[data.stock_symbol]) {
        console.warn('Invalid data or chart not found:', data);
        return;
    }

    const chart = charts[data.stock_symbol];
    const timestamp = new Date().toLocaleTimeString();

    // Add new data point
    chart.data.labels.push(timestamp);
    chart.data.datasets[0].data.push(data.current_price);

    // Maintain fixed window of data points
    if (chart.data.labels.length > maxDataPoints) {
        chart.data.labels.shift();
        chart.data.datasets[0].data.shift();
    }

    // Update chart with animation disabled for performance
    chart.update('none');
}

function getStockColor(symbol) {
    const stockConfig = {
        'AAPL': { color: '#ff6b6b', name: 'Apple' },
        'GOOGL': { color: '#4ecdc4', name: 'Google' },
        'AMZN': { color: '#45b7d1', name: 'Amazon' },
        'MSFT': { color: '#96ceb4', name: 'Microsoft' },
        'TSLA': { color: '#d4a373', name: 'Tesla' }
    };
    return stockConfig[symbol]?.color || '#007bff';
}

function addDataToContainer(data) {
    const container = document.getElementById('stockContent');
    const div = document.createElement('div');
    const color = getStockColor(data.stock_symbol);
    
    div.className = 'stock-item';
    div.style.borderLeftColor = color;
    div.innerHTML = `
        <div class="d-flex justify-content-between">
            <strong style="color: ${color}">${data.stock_symbol}</strong>
            <small class="text-muted">${new Date().toLocaleTimeString()}</small>
        </div>
        <div class="h5 mb-0">$${data.current_price.toFixed(2)}</div>
        <div class="small">
            Vol: ${data.volume.toLocaleString()}
        </div>
    `;
    
    container.insertBefore(div, container.firstChild);
    updateCounter();
}

function addSignalToContainer(signal) {
    const container = document.getElementById('signalContent');
    const signalConfig = getSignalConfig(signal.signal || 'HOLD');
    const div = document.createElement('div');
    
    div.className = 'p-2 border-bottom';
    div.innerHTML = `
        <div class="signal-indicator" style="background-color: ${signalConfig.color}; color: white; padding: 4px 8px; border-radius: 4px; display: inline-block;">
            ${signalConfig.symbol} ${signalConfig.text}
        </div>
        <div class="fw-bold mt-1">${signal.stock}</div>
        <div>$${signal.price.toFixed(2)}</div>
        <div class="small text-muted">
            ${(signal.confidence * 100).toFixed(1)}% confidence
        </div>
    `;
    
    container.insertBefore(div, container.firstChild);
}

function updateCounter() {
    const container = document.getElementById('stockContent');
    const count = container.children.length;
    document.getElementById('update-count').textContent = count;
}

function showNotification(message, type = 'info') {
    const existing = document.querySelector('.notification');
    if (existing) {
        existing.remove();
    }

    const notification = document.createElement('div');
    notification.className = `notification ${type}`;
    notification.innerHTML = message;
    document.body.appendChild(notification);

    setTimeout(() => {
        notification.style.opacity = '0';
        setTimeout(() => notification.remove(), 500);
    }, 5000);
}

// WebSocket event listeners
socket.on('live_stock_update', (data) => {
    updateChart(data);
    addDataToContainer(data);
});

socket.on('live_trading_signal', (signal) => {
    addSignalToChart(signal);
    addSignalToContainer(signal);
});

socket.on('connect', () => {
    showNotification('Connected to server', 'success');
});

socket.on('disconnect', () => {
    showNotification('Disconnected from server', 'error');
});

socket.on('connect_error', (error) => {
    showNotification(`Connection Error: ${error.message}`, 'warning');
});

// Historical data handling
const historicalCharts = {};

function initializeHistoricalControls() {
    const liveViewBtn = document.getElementById('liveViewBtn');
    const historicalViewBtn = document.getElementById('historicalViewBtn');
    const historicalControls = document.getElementById('historicalControls');
    const timePreset = document.getElementById('timePreset');
    const startTime = document.getElementById('startTime');
    const endTime = document.getElementById('endTime');
    const fetchBtn = document.getElementById('fetchHistorical');

    liveViewBtn.addEventListener('click', () => switchView('live'));
    historicalViewBtn.addEventListener('click', () => switchView('historical'));

    timePreset.addEventListener('change', (e) => {
        if (e.target.value) {
            const now = new Date();
            const hours = {
                '1h': 1, '4h': 4, '1d': 24, '7d': 168
            }[e.target.value];
            
            endTime.value = now.toISOString().slice(0, 16);
            const start = new Date(now - hours * 3600000);
            startTime.value = start.toISOString().slice(0, 16);
        }
    });

    fetchBtn.addEventListener('click', fetchHistoricalData);
}

function switchView(view) {
    const liveView = document.getElementById('liveView');
    const historicalView = document.getElementById('historicalView');
    const historicalControls = document.getElementById('historicalControls');
    const liveViewBtn = document.getElementById('liveViewBtn');
    const historicalViewBtn = document.getElementById('historicalViewBtn');

    if (view === 'historical') {
        liveView.style.display = 'none';
        historicalView.style.display = 'block';
        historicalControls.style.display = 'block';
        liveViewBtn.classList.remove('active');
        historicalViewBtn.classList.add('active');
    } else {
        liveView.style.display = 'block';
        historicalView.style.display = 'none';
        historicalControls.style.display = 'none';
        liveViewBtn.classList.add('active');
        historicalViewBtn.classList.remove('active');
    }
}

async function fetchHistoricalData() {
    const startTime = document.getElementById('startTime').value;
    const endTime = document.getElementById('endTime').value;

    if (!startTime || !endTime) {
        showNotification('Please select both start and end times', 'warning');
        return;
    }

    try {
        const response = await fetch(`/api/historical-data?start=${startTime}&end=${endTime}`);
        const data = await response.json();
        
        if (!data || Object.keys(data).length === 0) {
            showNotification('No data available for selected time range', 'warning');
            return;
        }

        updateHistoricalCharts(data);
        showNotification('Historical data loaded successfully', 'success');
    } catch (error) {
        console.error('Error fetching historical data:', error);
        showNotification('Error loading historical data', 'error');
    }
}

function updateHistoricalCharts(data) {
    const container = document.getElementById('historicalCharts');
    container.innerHTML = ''; // Clear existing charts

    Object.entries(data).forEach(([symbol, stockData]) => {
        // Create chart container
        const col = document.createElement('div');
        col.className = 'col-6 mb-4';
        col.innerHTML = `
            <div class="chart-card">
                <h5 class="chart-title">${symbol}</h5>
                <canvas id="historical-${symbol}"></canvas>
            </div>
        `;
        container.appendChild(col);

        // Initialize chart
        const ctx = document.getElementById(`historical-${symbol}`).getContext('2d');
        if (historicalCharts[symbol]) {
            historicalCharts[symbol].destroy();
        }

        historicalCharts[symbol] = new Chart(ctx, {
            type: 'line',
            data: {
                labels: stockData.map(d => new Date(d.timestamp).toLocaleString()),
                datasets: [{
                    label: `${symbol} Price`,
                    data: stockData.map(d => d.price),
                    borderColor: getStockColor(symbol),
                    backgroundColor: `${getStockColor(symbol)}33`,
                    borderWidth: 2,
                    fill: true
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        mode: 'index',
                        intersect: false
                    }
                },
                scales: {
                    y: {
                        beginAtZero: false,
                        ticks: {
                            callback: value => `$${value.toFixed(2)}`
                        }
                    }
                }
            }
        });
    });
}

// Initialize controls when page loads
document.addEventListener('DOMContentLoaded', () => {
    loadInitialData();
    initializeHistoricalControls();
});
