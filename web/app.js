const API_BASE = '/api';

let currentBusinessId = null;
let ratingTrendChart = null;
let sentimentDistChart = null;
let benchmarkChart = null;
let map = null;

async function init() {
    await loadBusinesses();
    initMap();
    loadGeoData();

    document.getElementById('business-select').addEventListener('change', (e) => {
        loadDashboard(e.target.value);
    });
}

async function loadBusinesses() {
    try {
        const res = await fetch(`${API_BASE}/businesses`);
        const businesses = await res.json();
        const select = document.getElementById('business-select');

        businesses.forEach(b => {
            const opt = document.createElement('option');
            opt.value = b.id;
            opt.textContent = `${b.name} (${b.industry})`;
            select.appendChild(opt);
        });

        if (businesses.length > 0) {
            loadDashboard(businesses[0].id);
        }
    } catch (e) {
        console.error("Failed to load businesses", e);
    }
}

async function loadDashboard(id) {
    currentBusinessId = id;
    loadOverview(id);
    loadDeltas(id);
    loadRatingTrend(id);
    loadSentimentDist(id);
    loadBenchmark(id);
}


async function loadOverview(id) {
    const res = await fetch(`${API_BASE}/business/${id}/overview`);
    const data = await res.json();

    const ts = data.trust_score;
    const tsEl = document.getElementById('trust-score');
    tsEl.textContent = ts;
    tsEl.className = 'metric ' + (ts >= 80 ? 'trust-score-good' : ts >= 60 ? 'trust-score-avg' : 'trust-score-bad');

    document.getElementById('trust-verification').textContent = `Industry Avg: ${Math.round(data.industry_avg_trust)}`;
    document.getElementById('rating').textContent = data.weighted_rating;
    document.getElementById('volume').textContent = data.total_reviews.toLocaleString();
    document.getElementById('response-rate').textContent = (data.response_rate * 100).toFixed(1) + '%';
}

async function loadDeltas(id) {
    const res = await fetch(`${API_BASE}/business/${id}/deltas`);
    const data = await res.json();

    const formatDelta = (val, type, isInverse = false) => {
        const el = document.getElementById(`delta-${type}`);
        const arrow = val > 0 ? '↑' : val < 0 ? '↓' : '-';
        const absVal = Math.abs(val);
        const text = val === 0 ? 'No change' : `${arrow} ${absVal.toFixed(2)}`;

        el.textContent = text;

        let colorClass = 'neutral-change';
        if (val !== 0) {
            if (isInverse) {
                colorClass = val < 0 ? 'positive-change' : 'negative-change';
            } else {
                colorClass = val > 0 ? 'positive-change' : 'negative-change';
            }
        }
        el.className = 'change-val ' + colorClass;
    };

    formatDelta(data.delta_rating, 'rating');
    formatDelta(data.delta_neg_sentiment, 'sentiment', true);
    formatDelta(data.delta_response_rate * 100, 'response');
}


function initMap() {
    map = L.map('map').setView([6.5, 3.35], 11);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
        subdomains: 'abcd',
        maxZoom: 19
    }).addTo(map);

    const legend = L.control({ position: 'bottomright' });
    legend.onAdd = function (map) {
        const div = L.DomUtil.create('div', 'legend');
        div.innerHTML += '<i style="background: #ef4444"></i> Negative Sentiment<br>';
        div.innerHTML += '<i style="background: #9ca3af"></i> Neutral<br>';
        div.innerHTML += '<i style="background: #10b981"></i> Positive Sentiment<br>';
        div.innerHTML += '<small>Size = Volume</small>';
        return div;
    };
    legend.addTo(map);
}

async function loadGeoData() {
    try {
        const res = await fetch(`${API_BASE}/geo/overview`);
        const data = await res.json();

        const insightRes = await fetch(`${API_BASE}/geo/insight`);
        const insightData = await insightRes.json();
        document.getElementById('map-insight').textContent = insightData.insight;

        const markers = [];

        data.forEach(d => {
            // Color Scale
            let color = '#9ca3af'; // Neutral
            if (d.net_sentiment_score > 0.1) color = '#10b981'; // Green
            if (d.net_sentiment_score < -0.1) color = '#ef4444'; // Red

            // Review counts range 5 to 100.
            const radius = Math.max(5, Math.log(d.review_count) * 4);

            const circle = L.circleMarker([d.latitude, d.longitude], {
                color: color,
                fillColor: color,
                fillOpacity: 0.7,
                radius: radius,
                weight: 1 // Stroke width
            }).bindPopup(`<b>${d.name}</b><br>Reviews: ${d.review_count}<br>Sentiment: ${d.net_sentiment_score.toFixed(2)}`);

            circle.addTo(map);
            markers.push(circle);
        });

        if (markers.length > 0) {
            const group = new L.featureGroup(markers);
            map.fitBounds(group.getBounds(), { padding: [50, 50] });
        }

    } catch (e) {
        console.error("Failed to load geo data", e);
    }
}

async function loadRatingTrend(id) {
    const res = await fetch(`${API_BASE}/business/${id}/rating-trend`);
    const data = await res.json();

    const labels = data.map(d => {
        const date = new Date(d.date);
        return date.toLocaleDateString(undefined, { month: 'short', year: '2-digit' });
    });
    const ratings = data.map(d => d.rating);

    if (ratingTrendChart) ratingTrendChart.destroy();

    const ctx = document.getElementById('ratingTrendChart').getContext('2d');
    ratingTrendChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Customer Rating Trend',
                    data: ratings,
                    borderColor: '#0f172a',
                    backgroundColor: 'rgba(15, 23, 42, 0.05)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.3,
                    pointRadius: 2
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                title: { display: true, text: 'Customer Rating Trend (Monthly)', align: 'start', padding: { bottom: 20 } }
            },
            scales: {
                y: {
                    min: 1,
                    max: 5,
                    grid: { borderDash: [4, 4] }
                },
                x: {
                    grid: { display: false }
                }
            }
        }
    });
}

async function loadSentimentDist(id) {
    const res = await fetch(`${API_BASE}/business/${id}/sentiment-dist`);
    const data = await res.json();

    const config = {
        labels: ['Positive', 'Neutral', 'Negative'],
        data: [data.Positive || 0, data.Neutral || 0, data.Negative || 0],
        colors: ['#10b981', '#cbd5e1', '#ef4444']
    };

    if (sentimentDistChart) sentimentDistChart.destroy();

    const ctx = document.getElementById('sentimentDistChart').getContext('2d');
    sentimentDistChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: config.labels,
            datasets: [{
                data: config.data,
                backgroundColor: config.colors,
                borderWidth: 0,
                hoverOffset: 4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '65%',
            plugins: {
                legend: { position: 'right', labels: { usePointStyle: true, boxWidth: 8 } },
                title: { display: true, text: 'Recent Sentiment (60d)', align: 'start' }
            }
        }
    });
}

async function loadBenchmark(id) {
    const res = await fetch(`${API_BASE}/business/${id}/benchmark`);
    const data = await res.json();
    const overviewRes = await fetch(`${API_BASE}/business/${id}/overview`);
    const overview = await overviewRes.json();

    if (benchmarkChart) benchmarkChart.destroy();

    const ctx = document.getElementById('benchmarkChart').getContext('2d');
    benchmarkChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['Trust Score', 'Response Rate %'],
            datasets: [
                {
                    label: 'This Business',
                    data: [overview.trust_score, overview.response_rate * 100],
                    backgroundColor: '#4338ca',
                    barPercentage: 0.6,
                    categoryPercentage: 0.8
                },
                {
                    label: 'Industry Avg (P50)',
                    data: [data.p50_trust_score, data.avg_response_rate * 100],
                    backgroundColor: '#e2e8f0',
                    barPercentage: 0.6,
                    categoryPercentage: 0.8
                },
                {
                    label: 'Top Performers (P90)',
                    data: [data.p90_trust_score, 95],
                    backgroundColor: '#f1f5f9',
                    barPercentage: 0.6,
                    categoryPercentage: 0.8
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: { display: true, text: 'Performance vs Industry', align: 'start' },
                legend: { position: 'top', align: 'end' }
            },
            scales: {
                y: { beginAtZero: true, max: 100, grid: { borderDash: [4, 4] } },
                x: { grid: { display: false } }
            }
        }
    });
}

init();
