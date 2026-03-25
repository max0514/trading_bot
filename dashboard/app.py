import sys
import os
import logging

# Add project root to path so scraper_in_pys can be imported
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import dash
from dash import html, dcc, dash_table, callback, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from datetime import datetime

from scraper_in_pys.mongo import Mongo
from scraper_in_pys.scraper_manager import ScraperManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize the scraper manager (shared across callbacks)
manager = ScraperManager()

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY],
    suppress_callback_exceptions=True,
    title='Trading Bot Dashboard',
    update_title='Loading...',
)

# ─────────────────────── Layout Components ───────────────────────

def make_stat_card(title, value_id, color='#58a6ff'):
    return dbc.Card([
        dbc.CardBody([
            html.Div(id=value_id, className='stat-number', style={'color': color}),
            html.Div(title, className='stat-label'),
        ], className='stat-card')
    ])


def make_scraper_row(name, display_name):
    return dbc.Card([
        dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    html.H6(display_name, className='mb-0'),
                    html.Span(id=f'status-badge-{name}', className='badge badge-idle'),
                ], width=4),
                dbc.Col([
                    dbc.Progress(id=f'progress-{name}', value=0, striped=True, animated=True,
                                 style={'height': '8px'}),
                    html.Small(id=f'progress-text-{name}', className='text-muted'),
                ], width=5),
                dbc.Col([
                    dbc.Button('Run', id=f'btn-run-{name}', color='primary', size='sm',
                               className='me-1'),
                ], width=3, className='text-end'),
            ], align='center'),
        ])
    ], className='mb-2')


# ─────────────────────── Main Layout ───────────────────────

app.layout = dbc.Container([
    # Interval for auto-refresh
    dcc.Interval(id='interval-refresh', interval=3000, n_intervals=0),
    dcc.Store(id='store-dummy'),

    # Navbar
    dbc.Navbar(
        dbc.Container([
            dbc.NavbarBrand([
                html.Span('📊 ', style={'fontSize': '1.4rem'}),
                'Trading Bot Dashboard'
            ]),
            dbc.Nav([
                dbc.NavItem(dbc.NavLink('Monitor', href='#', active=True, id='nav-monitor')),
                dbc.NavItem(dbc.NavLink('Data Explorer', href='#', id='nav-data')),
                dbc.NavItem(dbc.NavLink('News & Sentiment', href='#', id='nav-news')),
            ], navbar=True),
            html.Div([
                html.Small(id='last-refresh-time', className='text-muted'),
            ]),
        ], fluid=True),
        dark=True,
        className='navbar mb-3',
    ),

    # Tab content
    dbc.Tabs([
        # ── Tab 1: Scraper Monitor ──
        dbc.Tab(label='Scraper Monitor', children=[
            html.Div(className='mt-3', children=[
                # Stats row
                dbc.Row([
                    dbc.Col(make_stat_card('Stock Prices', 'stat-prices', '#58a6ff'), md=2),
                    dbc.Col(make_stat_card('Revenue Records', 'stat-revenue', '#3fb950'), md=2),
                    dbc.Col(make_stat_card('Quarterly Reports', 'stat-quarterly', '#bc8cff'), md=2),
                    dbc.Col(make_stat_card('News Articles', 'stat-news', '#d29922'), md=2),
                    dbc.Col(make_stat_card('PTT Posts', 'stat-ptt', '#f85149'), md=2),
                    dbc.Col(make_stat_card('Active Scrapers', 'stat-active', '#58a6ff'), md=2),
                ]),

                # Scraper controls
                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader([
                                'Scraper Controls',
                                dbc.Button('Run All', id='btn-run-all', color='success',
                                           size='sm', className='float-end'),
                            ]),
                            dbc.CardBody([
                                make_scraper_row('stock_price', '📈 Stock Price'),
                                make_scraper_row('monthly_revenue', '💰 Monthly Revenue'),
                                make_scraper_row('quarterly_report', '📋 Quarterly Report'),
                                make_scraper_row('news', '📰 News'),
                                make_scraper_row('ptt', '💬 PTT Forum'),
                            ]),
                        ]),
                    ], md=7),
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader('Activity Log'),
                            dbc.CardBody([
                                html.Div(id='log-panel', className='log-panel'),
                            ]),
                        ]),
                    ], md=5),
                ]),
            ]),
        ]),

        # ── Tab 2: Data Explorer ──
        dbc.Tab(label='Data Explorer', children=[
            html.Div(className='mt-3', children=[
                dbc.Row([
                    dbc.Col([
                        dbc.Label('Stock ID'),
                        dbc.Input(id='input-stock-id', type='text', value='2330',
                                  placeholder='e.g. 2330'),
                    ], md=2),
                    dbc.Col([
                        dbc.Label('Data Type'),
                        dbc.Select(
                            id='select-data-type',
                            options=[
                                {'label': 'Stock Price', 'value': 'stock_price'},
                                {'label': 'Monthly Revenue', 'value': 'month_revenue'},
                                {'label': 'Balance Sheet', 'value': 'balance_sheet'},
                                {'label': 'Income Sheet', 'value': 'income_sheet'},
                                {'label': 'Cash Flow', 'value': 'cash_flow'},
                            ],
                            value='stock_price',
                        ),
                    ], md=2),
                    dbc.Col([
                        dbc.Label('\u200b'),  # invisible spacer
                        html.Div([
                            dbc.Button('Load Data', id='btn-load-data', color='primary'),
                        ]),
                    ], md=2),
                ], className='mb-3'),

                # Chart
                dbc.Card([
                    dbc.CardBody([
                        dcc.Graph(id='chart-stock', config={'displayModeBar': True},
                                  style={'height': '500px'}),
                    ]),
                ]),

                # Data table
                dbc.Card([
                    dbc.CardHeader('Raw Data'),
                    dbc.CardBody([
                        html.Div(id='data-table-container'),
                    ]),
                ], className='mt-3'),
            ]),
        ]),

        # ── Tab 3: News & Sentiment ──
        dbc.Tab(label='News & Sentiment', children=[
            html.Div(className='mt-3', children=[
                dbc.Row([
                    dbc.Col([
                        dbc.Button('Refresh News', id='btn-refresh-news', color='primary',
                                   size='sm', className='mb-3'),
                    ]),
                ]),
                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader('Latest News'),
                            dbc.CardBody(id='news-feed'),
                        ]),
                    ], md=7),
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader('PTT Hot Posts (Stock Board)'),
                            dbc.CardBody(id='ptt-feed'),
                        ]),
                        dbc.Card([
                            dbc.CardHeader('PTT Post Type Distribution'),
                            dbc.CardBody([
                                dcc.Graph(id='chart-ptt-types', style={'height': '300px'}),
                            ]),
                        ], className='mt-3'),
                    ], md=5),
                ]),
            ]),
        ]),
    ]),
], fluid=True)


# ─────────────────────── Callbacks ───────────────────────

# Refresh scraper status periodically
@callback(
    [Output('stat-prices', 'children'),
     Output('stat-revenue', 'children'),
     Output('stat-quarterly', 'children'),
     Output('stat-news', 'children'),
     Output('stat-ptt', 'children'),
     Output('stat-active', 'children'),
     Output('last-refresh-time', 'children'),
     Output('log-panel', 'children')] +
    [Output(f'progress-{name}', 'value') for name in
     ['stock_price', 'monthly_revenue', 'quarterly_report', 'news', 'ptt']] +
    [Output(f'progress-text-{name}', 'children') for name in
     ['stock_price', 'monthly_revenue', 'quarterly_report', 'news', 'ptt']] +
    [Output(f'status-badge-{name}', 'children') for name in
     ['stock_price', 'monthly_revenue', 'quarterly_report', 'news', 'ptt']] +
    [Output(f'status-badge-{name}', 'className') for name in
     ['stock_price', 'monthly_revenue', 'quarterly_report', 'news', 'ptt']],
    Input('interval-refresh', 'n_intervals'),
)
def update_dashboard(n):
    statuses = manager.get_status()
    logs = manager.get_log(limit=30)

    # Count documents in each collection (cached approach - use status)
    counts = {}
    for name in ['stock_price', 'monthly_revenue', 'quarterly_report', 'news', 'ptt']:
        s = statuses.get(name, {})
        counts[name] = s.get('done', 0)

    # Active scrapers
    active = sum(1 for s in statuses.values() if s.get('running', False))

    # Build log entries
    log_children = []
    for entry in reversed(logs):
        ts = entry.get('timestamp', '')[:19]
        scraper = entry.get('scraper', '')
        msg = entry.get('message', '')
        level = entry.get('level', 'INFO')
        css_class = 'log-entry log-error' if level == 'ERROR' else 'log-entry'
        log_children.append(
            html.Div([
                html.Span(ts, className='log-time'),
                html.Span(f'[{scraper}]', className='log-scraper'),
                html.Span(msg, className='log-message'),
            ], className=css_class)
        )

    # Progress bars and badges
    progress_vals = []
    progress_texts = []
    badge_texts = []
    badge_classes = []
    scraper_names = ['stock_price', 'monthly_revenue', 'quarterly_report', 'news', 'ptt']

    for name in scraper_names:
        s = statuses.get(name, {})
        total = s.get('total', 0)
        done = s.get('done', 0)
        errors = s.get('errors', 0)
        running = s.get('running', False)

        pct = (done / total * 100) if total > 0 else 0
        progress_vals.append(pct)
        progress_texts.append(f'{done}/{total} (errors: {errors})')

        if running:
            badge_texts.append('Running')
            badge_classes.append('badge badge-running')
        elif errors > 0 and done > 0:
            badge_texts.append('Done (with errors)')
            badge_classes.append('badge badge-error')
        elif done > 0:
            badge_texts.append('Complete')
            badge_classes.append('badge badge-success')
        else:
            badge_texts.append('Idle')
            badge_classes.append('badge badge-idle')

    now = datetime.now().strftime('%H:%M:%S')

    return (
        ['-', '-', '-', '-', '-', str(active), f'Last update: {now}', log_children]
        + progress_vals + progress_texts + badge_texts + badge_classes
    )


# Run individual scrapers
for _name in ['stock_price', 'monthly_revenue', 'quarterly_report', 'news', 'ptt']:
    @callback(
        Output('store-dummy', 'data', allow_duplicate=True),
        Input(f'btn-run-{_name}', 'n_clicks'),
        prevent_initial_call=True,
    )
    def run_scraper(n_clicks, scraper_name=_name):
        if n_clicks:
            manager.run_scraper(scraper_name)
        return dash.no_update


# Run all scrapers
@callback(
    Output('store-dummy', 'data', allow_duplicate=True),
    Input('btn-run-all', 'n_clicks'),
    prevent_initial_call=True,
)
def run_all(n_clicks):
    if n_clicks:
        manager.run_all()
    return dash.no_update


# Data Explorer - load stock data
@callback(
    [Output('chart-stock', 'figure'),
     Output('data-table-container', 'children')],
    Input('btn-load-data', 'n_clicks'),
    [State('input-stock-id', 'value'),
     State('select-data-type', 'value')],
    prevent_initial_call=True,
)
def load_stock_data(n_clicks, stock_id, data_type):
    if not stock_id:
        return go.Figure(), html.P('Please enter a stock ID.')

    try:
        repo = Mongo(db='trading_bot', collection=data_type)
        df = repo.get_data_by_stock_id(str(stock_id))

        if df.empty:
            fig = go.Figure()
            fig.update_layout(
                template='plotly_dark',
                paper_bgcolor='#1c2128',
                plot_bgcolor='#0d1117',
                title=f'No data found for {stock_id}',
            )
            return fig, html.P(f'No data found for stock {stock_id} in {data_type}.')

        fig = go.Figure()
        fig.update_layout(
            template='plotly_dark',
            paper_bgcolor='#1c2128',
            plot_bgcolor='#0d1117',
            title=f'{data_type.replace("_", " ").title()} - {stock_id}',
            xaxis_title='Date',
            margin=dict(l=40, r=40, t=60, b=40),
        )

        if data_type == 'stock_price' and 'close' in df.columns:
            df['Timestamp'] = pd.to_datetime(df['Timestamp'])
            df = df.sort_values('Timestamp')

            fig.add_trace(go.Candlestick(
                x=df['Timestamp'],
                open=df.get('open', df['close']),
                high=df.get('max', df['close']),
                low=df.get('min', df['close']),
                close=df['close'],
                name='Price',
            ))

            if 'Trading_Volume' in df.columns:
                fig.add_trace(go.Bar(
                    x=df['Timestamp'],
                    y=df['Trading_Volume'],
                    name='Volume',
                    yaxis='y2',
                    opacity=0.3,
                    marker_color='#58a6ff',
                ))
                fig.update_layout(
                    yaxis2=dict(overlaying='y', side='right', showgrid=False),
                )

        elif data_type == 'month_revenue' and '當月營收' in df.columns:
            df['Timestamp'] = pd.to_datetime(df['Timestamp'])
            df = df.sort_values('Timestamp')

            fig.add_trace(go.Bar(
                x=df['Timestamp'],
                y=df['當月營收'],
                name='Monthly Revenue',
                marker_color='#3fb950',
            ))

            if '去年同月增減(%)' in df.columns:
                fig.add_trace(go.Scatter(
                    x=df['Timestamp'],
                    y=df['去年同月增減(%)'],
                    name='YoY Growth %',
                    yaxis='y2',
                    line=dict(color='#d29922', width=2),
                ))
                fig.update_layout(
                    yaxis2=dict(overlaying='y', side='right', title='YoY %', showgrid=False),
                )
        else:
            # Generic: show available numeric columns
            if 'Timestamp' in df.columns:
                df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
                df = df.sort_values('Timestamp')
            numeric_cols = df.select_dtypes(include='number').columns[:5]
            for col in numeric_cols:
                fig.add_trace(go.Scatter(
                    x=df.get('Timestamp', df.index),
                    y=df[col],
                    name=col,
                    mode='lines+markers',
                ))

        # Data table (show last 50 rows)
        display_df = df.tail(50)
        table = dash_table.DataTable(
            data=display_df.to_dict('records'),
            columns=[{'name': c, 'id': c} for c in display_df.columns],
            style_header={
                'backgroundColor': '#161b22',
                'color': '#e6edf3',
                'fontWeight': 'bold',
                'border': '1px solid #30363d',
            },
            style_data={
                'backgroundColor': '#1c2128',
                'color': '#e6edf3',
                'border': '1px solid #30363d',
            },
            style_table={'overflowX': 'auto'},
            page_size=20,
            sort_action='native',
            filter_action='native',
        )

        return fig, table

    except Exception as e:
        logger.error(f"Error loading data: {e}")
        fig = go.Figure()
        fig.update_layout(template='plotly_dark', paper_bgcolor='#1c2128', plot_bgcolor='#0d1117')
        return fig, html.P(f'Error loading data: {e}', style={'color': '#f85149'})


# News & Sentiment tab
@callback(
    [Output('news-feed', 'children'),
     Output('ptt-feed', 'children'),
     Output('chart-ptt-types', 'figure')],
    Input('btn-refresh-news', 'n_clicks'),
    prevent_initial_call=False,
)
def refresh_news(n_clicks):
    news_children = []
    ptt_children = []

    # Load news from DB
    try:
        news_repo = Mongo(db='trading_bot', collection='news')
        news_df = news_repo.get_recent_data(limit=30)

        if not news_df.empty:
            for _, row in news_df.iterrows():
                news_children.append(html.Div([
                    html.Span(row.get('source', ''), className='news-source'),
                    html.P(row.get('title', ''), className='news-title mb-0'),
                    html.Small(row.get('scraped_at', '')[:16], className='news-time'),
                ], className='news-item'))
        else:
            news_children = [html.P('No news yet. Click "Run" on the News scraper first.',
                                    className='text-muted')]
    except Exception:
        news_children = [html.P('Could not load news data.', className='text-muted')]

    # Load PTT posts from DB
    ptt_type_fig = go.Figure()
    ptt_type_fig.update_layout(
        template='plotly_dark',
        paper_bgcolor='#1c2128',
        plot_bgcolor='#0d1117',
        margin=dict(l=20, r=20, t=20, b=20),
    )

    try:
        ptt_repo = Mongo(db='trading_bot', collection='ptt_posts')
        ptt_df = ptt_repo.get_recent_data(limit=50)

        if not ptt_df.empty:
            for _, row in ptt_df.head(20).iterrows():
                push = row.get('push_count', '0')
                title = row.get('title', '')
                ptt_children.append(html.Div([
                    html.Span(f'[{push}] ', style={'color': '#3fb950', 'fontWeight': 'bold'}),
                    html.Span(title, style={'fontSize': '0.85rem'}),
                ], className='news-item'))

            # Post type chart
            if 'post_type' in ptt_df.columns:
                type_counts = ptt_df['post_type'].value_counts()
                ptt_type_fig = go.Figure(go.Pie(
                    labels=type_counts.index,
                    values=type_counts.values,
                    hole=0.4,
                    marker=dict(colors=['#58a6ff', '#3fb950', '#d29922', '#f85149', '#bc8cff', '#8b949e']),
                ))
                ptt_type_fig.update_layout(
                    template='plotly_dark',
                    paper_bgcolor='#1c2128',
                    plot_bgcolor='#0d1117',
                    margin=dict(l=20, r=20, t=20, b=20),
                    showlegend=True,
                    legend=dict(font=dict(size=10)),
                )
        else:
            ptt_children = [html.P('No PTT posts yet. Click "Run" on the PTT scraper first.',
                                   className='text-muted')]
    except Exception:
        ptt_children = [html.P('Could not load PTT data.', className='text-muted')]

    return news_children, ptt_children, ptt_type_fig


server = app.server

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8050)
