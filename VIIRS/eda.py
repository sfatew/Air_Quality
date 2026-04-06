"""
EDA: VIIRS AOD vs AQI Correlation Analysis
=============================================
So sánh AOD vệ tinh VIIRS (L2 nearest/radius, D3) với AQI tại 3 trạm Hà Nội.

Input:
  - VIIRS L2/D3 CSV từ viirs_aod_v4.py
  - AQI CSV: filtered_envisoft_air_quality_weather_data.csv

Output:
  - correlation_summary.csv
  - Scatter + regression, heatmap, time series, residual plots
"""

import os
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from scipy import stats
from sklearn.linear_model import LinearRegression, RANSACRegressor
from sklearn.metrics import r2_score, mean_absolute_error

warnings.filterwarnings("ignore")
sns.set(style="whitegrid")
plt.rcParams["figure.figsize"] = (14, 6)
plt.rcParams["axes.titlesize"] = 13

# ── CONFIG ──────────────────────────────────────────────────────────────────
VIIRS_OUTPUT_DIR = "/home/slow_data/Air_Quality/VIIRS/output"
AQI_CSV = "/home/slow_data/Air_Quality/filtered_envisoft_air_quality_weather_data.csv"
AQI_THRESH = 0
RANSAC_FRAC = 0.80
SAVE_DIR = "/home/slow_data/Air_Quality/VIIRS/output/correlation"

# Tên trạm VIIRS (ngắn) → tên folder output
STATIONS_FOLDER = {
    "HN_556_Nguyễn_Văn_Cừ": "HN: 556 Nguyễn Văn Cừ",
    "HN_CV_Nhân_Chính": "HN: CV Nhân Chính",
    "HN_ĐHBK_Giải_Phóng": "HN: ĐHBK Giải Phóng",
}

# Mapping tên VIIRS → tên trong AQI CSV
STATION_MAP_VIIRS_TO_AQI = {
    "HN: 556 Nguyễn Văn Cừ": "Hà Nội: 556 Nguyễn Văn Cừ (KK)",
    "HN: CV Nhân Chính": "Hà Nội: Công viên Nhân Chính - Khuất Duy Tiến (KK)",
    "HN: ĐHBK Giải Phóng": "Hà Nội: ĐHBK cổng Parabol đường Giải Phóng (KK)",
}

# Ngược lại
STATION_MAP_AQI_TO_VIIRS = {v: k for k, v in STATION_MAP_VIIRS_TO_AQI.items()}
# ────────────────────────────────────────────────────────────────────────────


# ===========================================================================
# 1. LOAD DATA
# ===========================================================================
def load_viirs_l2(method="nearest_pixel", platform="NOAA20"):
    """Load VIIRS L2 filtered CSV per station."""
    dfs = []
    base = os.path.join(VIIRS_OUTPUT_DIR, f"L2_{platform}", method)

    for folder_name, station_name in STATIONS_FOLDER.items():
        fpath = os.path.join(base, folder_name, "filtered.csv")
        if not os.path.exists(fpath):
            print(f"  Not found: {fpath}")
            continue
        df = pd.read_csv(fpath, parse_dates=["datetime"])
        df["station"] = station_name
        df["method"] = method
        dfs.append(df)
        print(f"  Loaded {method}/{folder_name}: {len(df)} records")

    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def load_viirs_d3(platform="NOAA20"):
    """Load VIIRS D3 CSV."""
    fpath = os.path.join(VIIRS_OUTPUT_DIR, f"viirs_aod_D3_{platform}.csv")
    if not os.path.exists(fpath):
        print(f"  D3 not found: {fpath}")
        return pd.DataFrame()
    df = pd.read_csv(fpath, parse_dates=["datetime"])
    df["method"] = "D3_daily"
    print(f"  Loaded D3: {len(df)} records")
    return df


def load_aqi(aqi_path=AQI_CSV):
    """
    Load AQI from envisoft CSV.
    Columns: Source, ID, Timestamp, Name, Latitude, Longitude, AQI, PM2.5, ...
    Timestamp is Vietnam local time (UTC+7).
    """
    df = pd.read_csv(aqi_path)

    # Parse datetime — try multiple formats
    if "Timestamp" in df.columns:
        df["datetime"] = pd.to_datetime(df["Timestamp"], format="%d/%m/%Y %H:%M", errors="coerce")
        if df["datetime"].isna().all():
            df["datetime"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    elif "timestamp" in df.columns:
        df["datetime"] = pd.to_datetime(df["timestamp"], errors="coerce")
    else:
        raise ValueError("No Timestamp/timestamp column in AQI CSV")

    # Convert Vietnam local time (UTC+7) → UTC to match VIIRS
    df["datetime"] = df["datetime"] - pd.Timedelta(hours=7)

    # Rename station column
    if "Name" in df.columns:
        df["station_aqi"] = df["Name"]
    elif "name" in df.columns:
        df["station_aqi"] = df["name"]

    # AQI to numeric
    df["AQI"] = pd.to_numeric(df["AQI"], errors="coerce")

    # Filter only 3 Hanoi stations
    hanoi_aqi_names = list(STATION_MAP_AQI_TO_VIIRS.keys())
    df = df[df["station_aqi"].isin(hanoi_aqi_names)].copy()

    # Map AQI station name → VIIRS station name
    df["station"] = df["station_aqi"].map(STATION_MAP_AQI_TO_VIIRS)

    # Keep useful columns
    keep_cols = ["datetime", "station", "station_aqi", "AQI",
                 "PM2.5", "PM10", "CO", "NO2", "O3", "SO2",
                 "Temperature", "Humidity", "Pressure", "Wind Speed"]
    keep_cols = [c for c in keep_cols if c in df.columns]
    df = df[keep_cols].dropna(subset=["station", "AQI", "datetime"])
    df = df.sort_values("datetime").reset_index(drop=True)

    print(f"  AQI loaded: {len(df)} records, stations: {df['station'].unique().tolist()}")
    print(f"  Date range: {df['datetime'].min()} → {df['datetime'].max()}")
    return df


# ===========================================================================
# 2. MERGE AOD + AQI
# ===========================================================================
def merge_aod_aqi(df_aod, df_aqi, time_window_hours=1):
    """
    Merge AOD and AQI by station + nearest datetime.
    L2: merge_asof within ±time_window_hours
    D3: merge by date (daily)
    """
    if df_aod.empty or df_aqi.empty:
        return pd.DataFrame()

    method = df_aod["method"].iloc[0] if "method" in df_aod.columns else "unknown"

    # Detect AOD column name
    if "aod" in df_aod.columns:
        aod_col = "aod"
    elif "aod_mean" in df_aod.columns:
        aod_col = "aod_mean"
    else:
        print("  Cannot find AOD column")
        return pd.DataFrame()

    if "D3" in method:
        # D3 is daily — merge by date
        df_aod = df_aod.copy()
        df_aqi_copy = df_aqi.copy()
        df_aod["date"] = df_aod["datetime"].dt.date
        df_aqi_copy["date"] = df_aqi_copy["datetime"].dt.date

        # Daily AQI average per station
        aqi_daily = df_aqi_copy.groupby(["date", "station"]).agg(
            AQI=("AQI", "mean"),
        ).reset_index()

        merged = df_aod.merge(aqi_daily, on=["date", "station"], how="inner")
    else:
        # L2 — merge by nearest time
        merged_list = []
        for station in df_aod["station"].unique():
            aod_s = df_aod[df_aod["station"] == station].copy().sort_values("datetime")
            aqi_s = df_aqi[df_aqi["station"] == station].copy().sort_values("datetime")
            if aqi_s.empty or aod_s.empty:
                continue

            m = pd.merge_asof(
                aod_s,
                aqi_s[["datetime", "AQI"]],
                on="datetime",
                direction="nearest",
                tolerance=pd.Timedelta(hours=time_window_hours),
            )
            merged_list.append(m)

        if not merged_list:
            return pd.DataFrame()
        merged = pd.concat(merged_list, ignore_index=True)

    merged = merged.dropna(subset=["AQI", aod_col])
    return merged


# ===========================================================================
# 3. REGRESSION
# ===========================================================================
def fit_ols_ransac(x, y, ransac_frac=0.80):
    x = np.array(x).reshape(-1, 1)
    y = np.array(y)

    ols = LinearRegression().fit(x, y)
    y_ols = ols.predict(x)

    min_s = max(2, int(len(x) * ransac_frac))
    ransac = RANSACRegressor(
        estimator=LinearRegression(), min_samples=min_s, random_state=42
    ).fit(x, y)
    inlier_mask = ransac.inlier_mask_
    y_ransac = ransac.predict(x)

    return dict(
        ols=ols, ransac=ransac, inlier_mask=inlier_mask,
        y_ols=y_ols, y_ransac=y_ransac,
        r2_ols=r2_score(y, y_ols),
        mae_ols=mean_absolute_error(y, y_ols),
        r2_ransac=r2_score(y[inlier_mask], y_ransac[inlier_mask]),
        mae_ransac=mean_absolute_error(y[inlier_mask], y_ransac[inlier_mask]),
        slope_ols=float(ols.coef_[0]),
        intercept_ols=float(ols.intercept_),
        slope_ransac=float(ransac.estimator_.coef_[0]),
        intercept_ransac=float(ransac.estimator_.intercept_),
    )


# ===========================================================================
# 4. PLOTS
# ===========================================================================
def plot_scatter_regression(merged, aod_col, method_name, save_dir):
    os.makedirs(save_dir, exist_ok=True)

    for station in merged["station"].unique():
        grp = merged[merged["station"] == station]
        sub = grp[["AQI", aod_col]].dropna()
        if len(sub) < 5:
            continue

        x = sub[aod_col].values
        y = sub["AQI"].values
        fit = fit_ols_ransac(x, y, RANSAC_FRAC)
        xs = np.linspace(x.min(), x.max(), 200)

        fig, ax = plt.subplots(figsize=(8, 6))
        fig.suptitle(f"{station}\n{method_name} — AOD vs AQI (n={len(sub)})", fontsize=12)

        ax.scatter(x[~fit["inlier_mask"]], y[~fit["inlier_mask"]],
                   color="lightcoral", alpha=0.5, s=20, label="Outliers")
        ax.scatter(x[fit["inlier_mask"]], y[fit["inlier_mask"]],
                   color="steelblue", alpha=0.6, s=20, label="Inliers")

        ax.plot(xs, fit["slope_ols"] * xs + fit["intercept_ols"],
                color="orange", lw=2,
                label=f"OLS: R²={fit['r2_ols']:.3f} MAE={fit['mae_ols']:.1f}")
        ax.plot(xs, fit["slope_ransac"] * xs + fit["intercept_ransac"],
                color="green", lw=2, linestyle="--",
                label=f"RANSAC: R²={fit['r2_ransac']:.3f} MAE={fit['mae_ransac']:.1f}")

        ax.set_xlabel("AOD 550nm")
        ax.set_ylabel("AQI")
        ax.legend(fontsize=8)
        plt.tight_layout()

        fname = f"scatter_{station.replace(' ', '_').replace(':', '')}.png"
        plt.savefig(os.path.join(save_dir, fname), dpi=150, bbox_inches="tight")
        plt.close()


def plot_scatter_all_stations(merged, aod_col, method_name, save_dir):
    """Single scatter plot with all stations combined."""
    os.makedirs(save_dir, exist_ok=True)
    sub = merged[["AQI", aod_col, "station"]].dropna()
    if len(sub) < 5:
        return

    x = sub[aod_col].values
    y = sub["AQI"].values
    fit = fit_ols_ransac(x, y, RANSAC_FRAC)
    xs = np.linspace(x.min(), x.max(), 200)

    fig, ax = plt.subplots(figsize=(9, 7))

    colors = {"HN: 556 Nguyễn Văn Cừ": "steelblue",
              "HN: CV Nhân Chính": "green",
              "HN: ĐHBK Giải Phóng": "darkorange"}

    for station, color in colors.items():
        mask_s = sub["station"] == station
        if mask_s.any():
            ax.scatter(sub.loc[mask_s, aod_col], sub.loc[mask_s, "AQI"],
                       color=color, alpha=0.5, s=20, label=station)

    ax.plot(xs, fit["slope_ols"] * xs + fit["intercept_ols"],
            color="red", lw=2,
            label=f"OLS: R²={fit['r2_ols']:.3f}")
    ax.plot(xs, fit["slope_ransac"] * xs + fit["intercept_ransac"],
            color="darkred", lw=2, linestyle="--",
            label=f"RANSAC: R²={fit['r2_ransac']:.3f}")

    pearson_r, _ = stats.pearsonr(x, y)
    ax.set_title(f"All Stations — {method_name}\nAOD vs AQI (n={len(sub)}, Pearson r={pearson_r:.3f})")
    ax.set_xlabel("AOD 550nm")
    ax.set_ylabel("AQI")
    ax.legend(fontsize=8)
    plt.tight_layout()
    plt.savefig(os.path.join(save_dir, "scatter_all_stations.png"), dpi=150, bbox_inches="tight")
    plt.close()


def plot_heatmap_pearson(all_results, save_dir):
    os.makedirs(save_dir, exist_ok=True)
    if all_results.empty:
        return

    pivot = all_results.pivot_table(index="station", columns="method", values="pearson_r")

    fig, ax = plt.subplots(figsize=(10, max(4, len(pivot) * 0.8)))
    sns.heatmap(pivot, annot=True, fmt=".3f", cmap="RdYlGn",
                center=0, vmin=-1, vmax=1, linewidths=0.5, ax=ax)
    ax.set_title(f"Pearson R: VIIRS AOD vs AQI (AQI>{AQI_THRESH})")
    ax.set_xlabel("Method")
    ax.set_ylabel("Station")
    plt.tight_layout()
    plt.savefig(os.path.join(save_dir, "heatmap_pearson_r.png"), dpi=150, bbox_inches="tight")
    plt.close()


def plot_r2_comparison(all_results, save_dir):
    os.makedirs(save_dir, exist_ok=True)
    if all_results.empty:
        return

    methods = all_results["method"].unique()
    n_methods = len(methods)
    fig, axes = plt.subplots(1, n_methods, figsize=(7 * n_methods, 5))
    if n_methods == 1:
        axes = [axes]

    for ax, method in zip(axes, methods):
        sub = all_results[all_results["method"] == method].sort_values("pearson_r", ascending=True)
        y_pos = range(len(sub))

        ax.barh([y - 0.2 for y in y_pos], sub["ols_r2"], height=0.35, color="orange", label="OLS R²")
        ax.barh([y + 0.2 for y in y_pos], sub["ransac_r2"], height=0.35, color="green", label="RANSAC R²")

        ax.set_yticks(list(y_pos))
        ax.set_yticklabels(sub["station"], fontsize=8)
        ax.set_xlabel("R²")
        ax.set_title(f"{method}: OLS vs RANSAC R²")
        ax.axvline(0, color="black", linewidth=0.8)
        ax.legend()

    plt.tight_layout()
    plt.savefig(os.path.join(save_dir, "r2_comparison.png"), dpi=150, bbox_inches="tight")
    plt.close()


def plot_timeseries_overlay(merged, aod_col, method_name, save_dir):
    os.makedirs(save_dir, exist_ok=True)

    for station in merged["station"].unique():
        grp = merged[merged["station"] == station].sort_values("datetime")
        if len(grp) < 3:
            continue

        fig, ax1 = plt.subplots(figsize=(16, 4))
        fig.suptitle(f"{station} — {method_name}", fontsize=12)

        ax1.plot(grp["datetime"], grp["AQI"], color="steelblue", marker="o", ms=3, label="AQI")
        ax1.set_ylabel("AQI", color="steelblue")
        ax1.tick_params(axis="y", labelcolor="steelblue")

        ax2 = ax1.twinx()
        ax2.plot(grp["datetime"], grp[aod_col], color="darkorange", marker="s", ms=3,
                 linestyle="--", label="AOD 550nm")
        ax2.set_ylabel("AOD", color="darkorange")
        ax2.tick_params(axis="y", labelcolor="darkorange")

        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=9)

        plt.xticks(rotation=30)
        plt.tight_layout()

        fname = f"timeseries_{station.replace(' ', '_').replace(':', '')}.png"
        plt.savefig(os.path.join(save_dir, fname), dpi=150, bbox_inches="tight")
        plt.close()


def plot_residuals(merged, aod_col, method_name, save_dir):
    os.makedirs(save_dir, exist_ok=True)
    sub = merged[["AQI", aod_col]].dropna()
    if len(sub) < 10:
        return

    x = sub[aod_col].values
    y = sub["AQI"].values
    fit = fit_ols_ransac(x, y, RANSAC_FRAC)

    res_ols = y - fit["y_ols"]
    res_ransac = y - fit["y_ransac"]

    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    fig.suptitle(f"Residual Analysis — {method_name}", fontsize=13)

    axes[0].scatter(fit["y_ols"], res_ols, alpha=0.4, s=12, color="orange", label="OLS")
    axes[0].scatter(fit["y_ransac"][fit["inlier_mask"]],
                    res_ransac[fit["inlier_mask"]],
                    alpha=0.4, s=12, color="green", label="RANSAC inliers")
    axes[0].axhline(0, color="black", linewidth=1)
    axes[0].set_xlabel("Fitted AQI")
    axes[0].set_ylabel("Residual")
    axes[0].set_title("Residuals vs Fitted")
    axes[0].legend()

    axes[1].hist(res_ols, bins=40, alpha=0.6, color="orange", label="OLS")
    axes[1].hist(res_ransac[fit["inlier_mask"]], bins=40, alpha=0.6, color="green", label="RANSAC")
    axes[1].set_xlabel("Residual")
    axes[1].set_ylabel("Count")
    axes[1].set_title("Residual Distribution")
    axes[1].legend()

    stats.probplot(res_ols, dist="norm", plot=axes[2])
    axes[2].set_title("Q-Q Plot (OLS)")

    plt.tight_layout()
    plt.savefig(os.path.join(save_dir, f"residuals_{method_name}.png"), dpi=150, bbox_inches="tight")
    plt.close()


def plot_monthly_correlation(merged, aod_col, method_name, save_dir):
    """Pearson R per month — seasonal pattern."""
    os.makedirs(save_dir, exist_ok=True)
    merged = merged.copy()
    merged["month"] = merged["datetime"].dt.month

    monthly_r = []
    for month in range(1, 13):
        sub = merged[merged["month"] == month][[aod_col, "AQI"]].dropna()
        if len(sub) >= 5:
            r, p = stats.pearsonr(sub[aod_col], sub["AQI"])
            monthly_r.append({"month": month, "pearson_r": r, "p_value": p, "n": len(sub)})

    if not monthly_r:
        return
    df_m = pd.DataFrame(monthly_r)

    fig, ax = plt.subplots(figsize=(10, 5))
    colors = ["#2e7d32" if r > 0.3 else "#ff9800" if r > 0 else "#c62828" for r in df_m["pearson_r"]]
    bars = ax.bar(df_m["month"], df_m["pearson_r"], color=colors, alpha=0.8)

    for bar, row in zip(bars, df_m.itertuples()):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.02,
                f"n={row.n}", ha="center", fontsize=8)

    ax.set_xticks(range(1, 13))
    ax.set_xticklabels(["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                         "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
    ax.set_ylabel("Pearson R")
    ax.set_title(f"Monthly Correlation: AOD vs AQI — {method_name}")
    ax.axhline(0, color="black", linewidth=0.5)
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(save_dir, f"monthly_correlation_{method_name}.png"), dpi=150, bbox_inches="tight")
    plt.close()


# ===========================================================================
# 5. MAIN
# ===========================================================================
def run_correlation_analysis(
    aqi_path: str = AQI_CSV,
    viirs_dir: str = VIIRS_OUTPUT_DIR,
    save_dir: str = SAVE_DIR,
    aqi_thresh: int = AQI_THRESH,
    platform: str = "NOAA20",
    time_window_hours: int = 1,
):
    """
    Full correlation analysis: VIIRS AOD (L2 nearest, L2 radius, D3) vs AQI.

    Parameters
    ----------
    aqi_path : str
        Path to envisoft AQI CSV
    time_window_hours : int
        Max time difference for L2-AQI matching (hours)
    """
    global VIIRS_OUTPUT_DIR
    VIIRS_OUTPUT_DIR = viirs_dir
    os.makedirs(save_dir, exist_ok=True)

    # Load AQI
    print("=" * 60)
    print("Loading AQI data...")
    df_aqi = load_aqi(aqi_path)
    if df_aqi.empty:
        print("No AQI data found for Hanoi stations!")
        return pd.DataFrame()

    # Methods to analyze
    methods = {
        "L2_nearest": {
            "loader": lambda: load_viirs_l2("nearest_pixel", platform),
            "aod_col": "aod",
        },
        "L2_radius_25km": {
            "loader": lambda: load_viirs_l2("radius_avg", platform),
            "aod_col": "aod_mean",
        },
        "D3_daily_1deg": {
            "loader": lambda: load_viirs_d3(platform),
            "aod_col": "aod_mean",
        },
    }

    all_results = []

    for method_name, cfg in methods.items():
        print(f"\n{'=' * 60}")
        print(f"Processing: {method_name}")
        print(f"{'=' * 60}")

        df_aod = cfg["loader"]()
        if df_aod.empty:
            print(f"  Skipping {method_name} — no data")
            continue

        aod_col = cfg["aod_col"]
        if aod_col not in df_aod.columns:
            print(f"  Column '{aod_col}' not in data. Available: {df_aod.columns.tolist()}")
            continue

        # Merge
        merged = merge_aod_aqi(df_aod, df_aqi, time_window_hours)
        if merged.empty:
            print(f"  No matched AOD-AQI pairs for {method_name}")
            continue

        # AQI threshold
        if aqi_thresh > 0:
            merged = merged[merged["AQI"] > aqi_thresh]

        print(f"  Matched pairs: {len(merged)} (AQI>{aqi_thresh})")
        print(f"  Stations: {merged['station'].unique().tolist()}")
        print(f"  AOD range: {merged[aod_col].min():.3f} – {merged[aod_col].max():.3f}")
        print(f"  AQI range: {merged['AQI'].min():.0f} – {merged['AQI'].max():.0f}")

        # Correlations per station
        for station in merged["station"].unique():
            grp = merged[merged["station"] == station]
            sub = grp[["AQI", aod_col]].dropna()
            if len(sub) < 5:
                continue

            x = sub[aod_col].values
            y = sub["AQI"].values

            pearson_r, pearson_p = stats.pearsonr(x, y)
            spearman_r, spearman_p = stats.spearmanr(x, y)

            try:
                fit = fit_ols_ransac(x, y, RANSAC_FRAC)
            except Exception:
                continue

            all_results.append(dict(
                station=station,
                method=method_name,
                n=len(sub),
                n_inliers=int(fit["inlier_mask"].sum()),
                pearson_r=round(pearson_r, 4),
                pearson_p=round(pearson_p, 4),
                spearman_r=round(spearman_r, 4),
                spearman_p=round(spearman_p, 4),
                ols_slope=round(fit["slope_ols"], 4),
                ols_intercept=round(fit["intercept_ols"], 3),
                ols_r2=round(fit["r2_ols"], 4),
                ols_mae=round(fit["mae_ols"], 3),
                ransac_slope=round(fit["slope_ransac"], 4),
                ransac_intercept=round(fit["intercept_ransac"], 3),
                ransac_r2=round(fit["r2_ransac"], 4),
                ransac_mae=round(fit["mae_ransac"], 3),
            ))

        # Plots per method
        method_save = os.path.join(save_dir, method_name)
        plot_scatter_regression(merged, aod_col, method_name, method_save)
        plot_scatter_all_stations(merged, aod_col, method_name, method_save)
        plot_timeseries_overlay(merged, aod_col, method_name, method_save)
        plot_residuals(merged, aod_col, method_name, method_save)
        plot_monthly_correlation(merged, aod_col, method_name, method_save)

    # Summary
    df_results = pd.DataFrame(all_results)
    if df_results.empty:
        print("\nNo correlation results. Check data overlap between VIIRS and AQI dates.")
        return df_results

    # Save summary
    df_results.to_csv(os.path.join(save_dir, "correlation_summary.csv"), index=False)

    # Summary plots
    plot_heatmap_pearson(df_results, save_dir)
    plot_r2_comparison(df_results, save_dir)

    # Print
    print("\n" + "=" * 70)
    print("CORRELATION SUMMARY")
    print("=" * 70)
    print(f"{'Station':32s} {'Method':18s} {'n':>5s} {'Pearson':>8s} {'Spearman':>9s} {'OLS R²':>7s} {'RANSAC R²':>10s}")
    print("-" * 95)
    for _, r in df_results.iterrows():
        print(f"{r['station']:32s} {r['method']:18s} {r['n']:5d} {r['pearson_r']:+8.3f} {r['spearman_r']:+9.3f} {r['ols_r2']:7.3f} {r['ransac_r2']:10.3f}")
    print("=" * 70)

    best = df_results.loc[df_results["pearson_r"].abs().idxmax()]
    print(f"\nBest: {best['station']} × {best['method']} (Pearson r={best['pearson_r']:.3f}, n={best['n']})")

    return df_results


if __name__ == "__main__":
    df_results = run_correlation_analysis(
        aqi_path="/home/slow_data/Air_Quality/filtered_envisoft_air_quality_weather_data.csv",
        viirs_dir="/home/slow_data/Air_Quality/VIIRS/output",
        save_dir="/home/slow_data/Air_Quality/VIIRS/output/correlation",
        aqi_thresh=0,
        platform="NOAA20",
        time_window_hours=1,
    )