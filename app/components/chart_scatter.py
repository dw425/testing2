"""Scatter plot chart component for ManufacturingIQ Databricks app."""

from dash import dcc
import plotly.graph_objects as go


def make_scatter_chart(data, x_col, y_col, color_col, title):
    """Create a dark-themed scatter chart distinguishing normal vs anomaly points.

    Points are split based on the *color_col* column value.  Rows whose
    ``color_col`` value is one of the recognised anomaly labels
    (``"Anomaly"``, ``"anomaly"``, ``"Critical"``, ``"Alert"``,
    ``"Fail"``, ``True``, or ``1``) are rendered as red triangles; all
    remaining rows are rendered as blue circles.

    Args:
        data: A pandas DataFrame (or list-of-dicts convertible to one).
        x_col: Column name to use for the x-axis.
        y_col: Column name to use for the y-axis.
        color_col: Column name whose values determine normal vs anomaly.
        title: Chart title string.

    Returns:
        A dcc.Graph component with the configured scatter plot.
    """
    import pandas as pd

    # Ensure we have a DataFrame
    if not isinstance(data, pd.DataFrame):
        data = pd.DataFrame(data)

    # Define which values are treated as anomalies
    anomaly_values = {"Anomaly", "anomaly", "Critical", "Alert", "Fail", True, 1, "1", "True"}

    is_anomaly = data[color_col].isin(anomaly_values)
    normal_data = data[~is_anomaly]
    anomaly_data = data[is_anomaly]

    fig = go.Figure()

    # Normal points - blue circles
    if not normal_data.empty:
        fig.add_trace(
            go.Scatter(
                x=normal_data[x_col],
                y=normal_data[y_col],
                mode="markers",
                name="Normal",
                marker=dict(
                    color="#3A7BF7",
                    size=8,
                    symbol="circle",
                    line=dict(width=1, color="#2A5FC0"),
                    opacity=0.8,
                ),
                hovertemplate=(
                    f"<b>{x_col}:</b> %{{x}}<br>"
                    f"<b>{y_col}:</b> %{{y}}<br>"
                    "<extra>Normal</extra>"
                ),
            )
        )

    # Anomaly points - red triangles
    if not anomaly_data.empty:
        fig.add_trace(
            go.Scatter(
                x=anomaly_data[x_col],
                y=anomaly_data[y_col],
                mode="markers",
                name="Anomaly",
                marker=dict(
                    color="#E74C3C",
                    size=10,
                    symbol="triangle-up",
                    line=dict(width=1, color="#C0392B"),
                    opacity=0.9,
                ),
                hovertemplate=(
                    f"<b>{x_col}:</b> %{{x}}<br>"
                    f"<b>{y_col}:</b> %{{y}}<br>"
                    "<extra>Anomaly</extra>"
                ),
            )
        )

    # Dark theme layout
    fig.update_layout(
        title=dict(
            text=title,
            font=dict(
                size=16,
                color="#EAEBF0",
                family="'Inter', 'Segoe UI', -apple-system, sans-serif",
            ),
            x=0.0,
            xanchor="left",
            pad=dict(l=10, t=10),
        ),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(
            color="#A0A4B0",
            family="'Inter', 'Segoe UI', -apple-system, sans-serif",
            size=12,
        ),
        xaxis=dict(
            title=dict(
                text=x_col,
                font=dict(color="#8A8D98", size=12),
            ),
            gridcolor="#272A31",
            gridwidth=1,
            zeroline=False,
            linecolor="#272A31",
            linewidth=1,
            tickfont=dict(color="#6C6F7A", size=11),
        ),
        yaxis=dict(
            title=dict(
                text=y_col,
                font=dict(color="#8A8D98", size=12),
            ),
            gridcolor="#272A31",
            gridwidth=1,
            zeroline=False,
            linecolor="#272A31",
            linewidth=1,
            tickfont=dict(color="#6C6F7A", size=11),
        ),
        legend=dict(
            font=dict(color="#A0A4B0", size=12),
            bgcolor="rgba(0,0,0,0)",
            bordercolor="#272A31",
            borderwidth=1,
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        margin=dict(l=60, r=20, t=60, b=50),
        hovermode="closest",
        hoverlabel=dict(
            bgcolor="#1E2028",
            font_size=12,
            font_color="#EAEBF0",
            bordercolor="#3A7BF7",
        ),
    )

    chart = dcc.Graph(
        id=f"scatter-{title.lower().replace(' ', '-')}",
        figure=fig,
        config={
            "displayModeBar": True,
            "modeBarButtonsToRemove": ["lasso2d", "select2d"],
            "displaylogo": False,
        },
        style={
            "backgroundColor": "#16181D",
            "border": "1px solid #272A31",
            "borderRadius": "6px",
            "padding": "8px",
        },
    )

    return chart
