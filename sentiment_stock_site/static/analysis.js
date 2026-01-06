const API = "http://127.0.0.1:8000";
let chart;

async function loadTickers() {
    const res = await fetch(`${API}/tickers`);
    const tickers = await res.json();
    const select = document.getElementById("ticker");

    tickers.forEach(t => {
        const opt = document.createElement("option");
        opt.value = t;
        opt.text = t;
        select.add(opt);
    });
}

async function loadChart() {
    const ticker = document.getElementById("ticker").value;
    const days = document.getElementById("days").value;

    const res = await fetch(`${API}/history?ticker=${ticker}&days=${days}`);
    const data = await res.json();

    if (chart) chart.destroy();

    chart = new Chart(document.getElementById("analysisChart"), {
        type: "line",
        data: {
            labels: data.dates,
            datasets: [
                {
                    label: "Цена",
                    data: data.close,
                    yAxisID: "y"
                },
                {
                    label: "Sentiment",
                    data: data.sentiment,
                    yAxisID: "y1"
                }
            ]
        },
        options: {
            scales: {
                y: { position: "left" },
                y1: {
                    position: "right",
                    grid: { drawOnChartArea: false }
                }
            }
        }
    });
}

loadTickers();