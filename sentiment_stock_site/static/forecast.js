const API = "http://localhost:8000";
let chart;

async function runForecast() {
    try {
        const days = parseInt(document.getElementById("days").value, 10) || 5;
        const sentiment = parseInt(document.getElementById("sentiment").value, 10) || 0;
        
        console.log(`Making forecast request: days=${days}, sentiment=${sentiment}`);
        
        // Показываем индикатор загрузки
        showLoadingIndicator();
        
        // Тестируем соединение
        console.log("Testing connection...");
        const testRes = await fetch(`${API}/health`);
        if (!testRes.ok) {
            throw new Error(`Server not responding: ${testRes.status}`);
        }
        
        // Получаем историю
        console.log("Fetching history...");
        const histRes = await fetch(`${API}/history?days=30`);
        if (!histRes.ok) {
            throw new Error(`History error: ${histRes.status}`);
        }
        const histData = await histRes.json();
        
        if (histData.error) {
            throw new Error(`History error: ${histData.error}`);
        }
        
        console.log(`History received: ${histData.dates.length} days, last price: ${histData.close[histData.close.length - 1]}`);
        
        // Получаем прогноз
        console.log("Fetching forecast...");
        const forecastRes = await fetch(`${API}/api/forecast?n_days=${days}&sentiment=${sentiment}`);
        if (!forecastRes.ok) {
            throw new Error(`Forecast error: ${forecastRes.status}`);
        }
        
        const forecast = await forecastRes.json();
        console.log("Forecast response:", forecast);
        
        if (forecast.error) {
            throw new Error(`Forecast error: ${forecast.error}`);
        }
        
        if (!forecast.forecast || !Array.isArray(forecast.forecast)) {
            throw new Error("Invalid forecast format");
        }
        
        // Создаем правильные данные для графика
        const historicalDays = Math.min(30, histData.dates.length);
        
        // Исторические данные (реальные)
        const historicalDates = histData.dates.slice(-historicalDays);
        const historicalPrices = histData.close.slice(-historicalDays);
        
        // Прогнозные данные
        const forecastPrices = forecast.forecast;
        
        // Создаем метки для прогноза
        const lastHistoricalDate = new Date(historicalDates[historicalDates.length - 1]);
        const forecastDates = [];
        
        for (let i = 1; i <= forecastPrices.length; i++) {
            const nextDate = new Date(lastHistoricalDate);
            nextDate.setDate(nextDate.getDate() + i);
            forecastDates.push(nextDate.toISOString().split('T')[0]); // YYYY-MM-DD
        }
        
        // Все метки
        const allLabels = [...historicalDates, ...forecastDates];
        
        // Данные для графиков
        const historicalData = [...historicalPrices, ...Array(forecastPrices.length).fill(null)];
        const forecastData = [...Array(historicalPrices.length - 1).fill(null), historicalPrices[historicalPrices.length - 1], ...forecastPrices];
        
        console.log("Chart data prepared:");
        console.log("- Historical prices:", historicalPrices.slice(-5));
        console.log("- Forecast prices:", forecastPrices);
        console.log("- Last historical date:", lastHistoricalDate);
        console.log("- First forecast date:", forecastDates[0]);
        
        // Создаем/обновляем график
        createOrUpdateChart(
            allLabels,
            historicalData,
            forecastData,
            sentiment,
            forecast
        );
        
        // Показываем информацию о прогнозе
        updateForecastInfo(historicalPrices, forecast, sentiment);
        
    } catch (err) {
        console.error('Forecast error:', err);
        showError(`Ошибка: ${err.message}`);
        createErrorChart();
    }
}

function createOrUpdateChart(labels, historicalData, forecastData, sentiment, forecast) {
    const ctx = document.getElementById('forecastChart').getContext('2d');
    
    // Уничтожаем старый график если есть
    if (chart) {
        chart.destroy();
    }
    
    // Определяем цвет для sentiment
    let forecastColor = 'rgb(255, 99, 132)'; // Красный для негативного
    if (sentiment > 0) {
        forecastColor = 'rgb(75, 192, 75)'; // Зеленый для позитивного
    } else if (sentiment === 0) {
        forecastColor = 'rgb(255, 159, 64)'; // Оранжевый для нейтрального
    }
    
    chart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Историческая цена',
                    data: historicalData,
                    borderColor: 'rgb(54, 162, 235)',
                    backgroundColor: 'rgba(54, 162, 235, 0.1)',
                    borderWidth: 2,
                    tension: 0.2,
                    fill: false,
                    pointRadius: 3,
                    pointHoverRadius: 5
                },
                {
                    label: 'Прогноз',
                    data: forecastData,
                    borderColor: forecastColor,
                    backgroundColor: forecastColor.replace('rgb', 'rgba').replace(')', ', 0.1)'),
                    borderWidth: 3,
                    borderDash: [5, 5],
                    tension: 0.2,
                    fill: false,
                    pointRadius: 4,
                    pointHoverRadius: 6,
                    pointStyle: 'triangle'
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    position: 'top',
                    labels: {
                        font: {
                            size: 14
                        }
                    }
                },
                title: {
                    display: true,
                    text: `Прогноз цены акции (Sentiment: ${getSentimentText(sentiment)})`,
                    font: {
                        size: 16,
                        weight: 'bold'
                    },
                    padding: {
                        top: 10,
                        bottom: 30
                    }
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    callbacks: {
                        label: function(context) {
                            let label = context.dataset.label || '';
                            if (label) {
                                label += ': ';
                            }
                            if (context.parsed.y !== null) {
                                label += new Intl.NumberFormat('ru-RU', {
                                    style: 'currency',
                                    currency: 'USD'
                                }).format(context.parsed.y);
                            }
                            return label;
                        }
                    }
                }
            },
            scales: {
                x: {
                    title: {
                        display: true,
                        text: 'Дата',
                        font: {
                            size: 14,
                            weight: 'bold'
                        }
                    },
                    grid: {
                        display: true,
                        color: 'rgba(0, 0, 0, 0.1)'
                    },
                    ticks: {
                        maxRotation: 45,
                        minRotation: 45
                    }
                },
                y: {
                    title: {
                        display: true,
                        text: 'Цена ',
                        font: {
                            size: 14,
                            weight: 'bold'
                        }
                    },
                    beginAtZero: false,
                    grid: {
                        display: true,
                        color: 'rgba(0, 0, 0, 0.1)'
                    },
                    ticks: {
                        callback: function(value) {
                            return value.toFixed(2);
                        }
                    }
                }
            },
            interaction: {
                intersect: false,
                mode: 'nearest'
            },
            elements: {
                line: {
                    tension: 0.2
                }
            }
        }
    });
}

function getSentimentText(sentiment) {
    switch(sentiment) {
        case 1: return 'Позитивный';
        case -1: return 'Негативный';
        default: return 'Нейтральный';
    }
}

function updateForecastInfo(historicalPrices, forecast, sentiment) {
    // Удаляем старую информацию
    const oldInfo = document.getElementById('forecast-info');
    if (oldInfo) oldInfo.remove();
    
    const lastPrice = historicalPrices[historicalPrices.length - 1];
    const forecastValues = forecast.forecast;
    const forecastMin = Math.min(...forecastValues);
    const forecastMax = Math.max(...forecastValues);
    const avgForecast = forecastValues.reduce((a, b) => a + b, 0) / forecastValues.length;
    const firstDayChange = ((forecastValues[0] - lastPrice) / lastPrice * 100).toFixed(2);
    const totalChange = ((avgForecast - lastPrice) / lastPrice * 100).toFixed(2);
    
    const sentimentEmoji = sentiment === 1 ? '📈' : sentiment === -1 ? '📉' : '➡️';
    const sentimentClass = sentiment === 1 ? 'positive' : sentiment === -1 ? 'negative' : 'neutral';
    
    const infoHTML = `
        <div id="forecast-info" class="forecast-info ${sentimentClass}">
            <h3>${sentimentEmoji} Сводка прогноза</h3>
            <div class="forecast-stats">
                <div class="stat-item">
                    <span class="stat-label">Текущая цена:</span>
                    <span class="stat-value">${lastPrice.toFixed(2)}</span>
                </div>
                <div class="stat-item">
                    <span class="stat-label">Изменение (1 день):</span>
                    <span class="stat-value ${firstDayChange >= 0 ? 'positive' : 'negative'}">
                        ${firstDayChange >= 0 ? '+' : ''}${firstDayChange}%
                    </span>
                </div>
                <div class="stat-item">
                    <span class="stat-label">Средний прогноз:</span>
                    <span class="stat-value">${avgForecast.toFixed(2)}</span>
                </div>
                <div class="stat-item">
                    <span class="stat-label">Диапазон прогноза:</span>
                    <span class="stat-value">${forecastMin.toFixed(2)} - ${forecastMax.toFixed(2)}</span>
                </div>
                <div class="stat-item">
                    <span class="stat-label">Общее изменение:</span>
                    <span class="stat-value ${totalChange >= 0 ? 'positive' : 'negative'}">
                        ${totalChange >= 0 ? '+' : ''}${totalChange}%
                    </span>
                </div>
                <div class="stat-item">
                    <span class="stat-label">Модель:</span>
                    <span class="stat-value">${forecast.model || 'упрощенная'}</span>
                </div>
            </div>
        </div>
    `;
    
    const container = document.querySelector('.container');
    const buttonCard = document.querySelector('.card:first-child');
    container.insertBefore(createElementFromHTML(infoHTML), buttonCard.nextSibling);
}

function showLoadingIndicator() {
    const chartCanvas = document.getElementById("forecastChart");
    const ctx = chartCanvas.getContext('2d');
    
    // Очищаем canvas
    ctx.clearRect(0, 0, chartCanvas.width, chartCanvas.height);
    
    // Рисуем фон
    ctx.fillStyle = '#f8f9fa';
    ctx.fillRect(0, 0, chartCanvas.width, chartCanvas.height);
    
    // Текст загрузки
    ctx.fillStyle = '#6c757d';
    ctx.font = '18px Arial';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText('Загрузка прогноза...', chartCanvas.width / 2, chartCanvas.height / 2 - 20);
    
    // Индикатор
    ctx.font = '14px Arial';
    ctx.fillText('⏳', chartCanvas.width / 2, chartCanvas.height / 2 + 20);
}

function createErrorChart() {
    const ctx = document.getElementById('forecastChart').getContext('2d');
    
    if (chart) chart.destroy();
    
    chart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: ['Ошибка'],
            datasets: [{
                label: 'Нет данных',
                data: [0],
                borderColor: 'rgba(200, 200, 200, 0.5)',
                backgroundColor: 'rgba(200, 200, 200, 0.1)'
            }]
        },
        options: {
            plugins: {
                legend: { display: false },
                title: {
                    display: true,
                    text: 'Ошибка загрузки данных'
                }
            }
        }
    });
}

function createElementFromHTML(htmlString) {
    const div = document.createElement('div');
    div.innerHTML = htmlString.trim();
    return div.firstChild;
}

function showError(message) {
    // Удаляем старые ошибки
    const oldError = document.getElementById('error-message');
    if (oldError) oldError.remove();
    
    const errorDiv = document.createElement('div');
    errorDiv.id = 'error-message';
    errorDiv.className = 'error-message';
    errorDiv.innerHTML = `
        <div class="error-content">
            <strong>Ошибка:</strong> ${message}
        </div>
    `;
    
    const container = document.querySelector('.container');
    container.insertBefore(errorDiv, container.firstChild);
    
    // Автоматически скрываем через 5 секунд
    setTimeout(() => {
        if (errorDiv.parentNode) {
            errorDiv.parentNode.removeChild(errorDiv);
        }
    }, 5000);
}

// Автоматический запуск прогноза при загрузке страницы
document.addEventListener('DOMContentLoaded', function() {
    console.log('Forecast page loaded');
    
    // Проверяем доступность API
    fetch(`${API}/health`)
        .then(res => res.json())
        .then(data => {
            console.log('Server health:', data);
            if (data.status === 'healthy') {
                // Запускаем прогноз с параметрами по умолчанию
                setTimeout(runForecast, 500);
            } else {
                showError('Сервер не работает. Проверьте бэкенд.');
            }
        })
        .catch(err => {
            console.error('Server not reachable:', err);
            showError('Не удается подключиться к серверу. Убедитесь, что бэкенд запущен на порту 8000.');
        });
});