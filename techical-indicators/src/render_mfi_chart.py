import traceback

import yfinance as yf
from talib import abstract
import json
import jsonschema

compute_mfi_schema = {
    'type': 'object',
    'properties': {
        'symbol': {'type': 'string'},
        'start_date': {'type': 'string', 'pattern': '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'},
        'end_date': {'type': 'string', 'pattern': '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'}
    },
    'required': ['symbol', 'start_date', 'end_date']
}


def compute_mfi(stock_data_df):
    stock_data_df.columns = [
        'open', 'high', 'low', 'close', 'adj close', 'volume'
    ]
    stock_data_df.drop(columns=['adj close'], inplace=True)
    mfi = abstract.Function('mfi')
    result = mfi(stock_data_df, timeperiod=14)
    return result


def main(event, context):
    print(event)
    # Step 1. Extract and validate input from request query string
    try:
        query_string_params = event.get('queryStringParameters', None)
        if query_string_params is None:
            raise Exception('query string is required')
        jsonschema.validate(query_string_params, compute_mfi_schema)
    except Exception as e:
        return create_html_error(400, e)

    try:
        # Step 2. Download Ticker
        symbol = query_string_params.get('symbol')
        tickers = [{'ticker': symbol, 'color': 'rgb(34,139,34)'}, {'ticker': '^IXIC', 'color': 'rgb(192,192,192)'}]
        start_date = query_string_params.get('start_date')
        end_date = query_string_params.get('end_date')

        datasets = []
        data = yf.download([item['ticker'] for item in tickers], start=start_date, end=end_date, group_by='ticker')

        # Step 3. Compute Money Flow Index
        for item in tickers:
            mfi = compute_mfi(data[item['ticker']])

            # Prepare dataset for each stock's MFI
            datasets.append({
                "label": item['ticker'] + " MFI",
                "data": mfi.tolist(),
                "fill": False,
                "borderColor": f"{item['color']}",
                "pointStyle": False
            })

        # Step 4. Prepare chart.js chart data object
        chart_data = {
            "type": "line",
            "data": {
                "labels": [str(date.date()) for date in data.index],
                "datasets": datasets
            },
            "options": {
                "title": {
                    "display": True,
                    "text": 'Money Flow Index (MFI) of Stocks'
                },
            }
        }
        return create_html_response(chart_data)
    except Exception as e:
        print(e)
        return create_html_error(500, e)


def create_html_response(chart_data):
    html_content = f"""
    <div>
        <canvas id="stockChart"></canvas>
    </div>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script>
        var ctx = document.getElementById('stockChart').getContext('2d');
        var chartData = {json.dumps(chart_data)};
        new Chart(ctx, chartData);
    </script>
    """

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "text/html",
        },
        "body": html_content,
    }


def create_html_error(code, exception):
    html_content = f"""
    <h3>Error</h3>
    <pre>{str(exception)}</pre>
    <pre>{traceback.format_exc()}</pre>
    """
    return {
        'statusCode': code,
        "headers": {
            "Content-Type": "text/html",
        },
        'body': html_content
    }
