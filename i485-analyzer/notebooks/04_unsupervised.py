#!/usr/bin/env python3
"""
I-485 Fraud Detection — Unsupervised Anomaly Detection Models

Trains 7 unsupervised/semi-supervised models on the feature matrix produced by
the feature-engineering pipeline, outputs anomaly scores for every application.

Models:
  3A. Isolation Forest           — full-data unsupervised
  3B. Autoencoder                — semi-supervised (train on clean only)
  3C. AE + IF Hybrid             — latent-space anomaly detection
  3D. Local Outlier Factor       — novelty detection on financial features
  3E. COPOD                      — copula-based outlier detection
  3F. Mahalanobis Distance       — robust covariance on financial features
  3G. Benford's Law              — first-digit distribution analysis

Usage:
    python notebooks/04_unsupervised.py
"""
import os
import sys
import time
import logging
import warnings
import numpy as np
import pandas as pd
import joblib
from pathlib import Path
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from sklearn.preprocessing import StandardScaler
from sklearn.covariance import MinCovDet, EmpiricalCovariance
from scipy.stats import chi2

warnings.filterwarnings("ignore", category=FutureWarning)

# ---------------------------------------------------------------------------
#  Logging
# ---------------------------------------------------------------------------
LOG = logging.getLogger("unsupervised")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)

# ---------------------------------------------------------------------------
#  Paths (all relative to project root i485-analyzer/)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
FEATURE_PATH = PROJECT_ROOT / "data" / "features" / "feature_matrix.parquet"
LABELS_PATH = PROJECT_ROOT / "data" / "features" / "labels.parquet"
RESULTS_DIR = PROJECT_ROOT / "data" / "results"
MODELS_DIR = PROJECT_ROOT / "data" / "models"

# ---------------------------------------------------------------------------
#  Optional dependency checks
# ---------------------------------------------------------------------------
HAS_TORCH = False
try:
    import torch
    import torch.nn as nn
    from torch.utils.data import DataLoader, TensorDataset
    HAS_TORCH = True
except ImportError:
    LOG.warning("PyTorch not available — skipping Autoencoder (3B) and Hybrid (3C) models")

HAS_PYOD = False
try:
    from pyod.models.copod import COPOD
    HAS_PYOD = True
except ImportError:
    LOG.warning("pyod not available — skipping COPOD (3E) model")


# ═══════════════════════════════════════════════════════════════════════════════
#  Utilities
# ═══════════════════════════════════════════════════════════════════════════════

def normalize_scores(scores: np.ndarray) -> np.ndarray:
    """Normalize an array to [0, 1].  Higher = more anomalous."""
    s_min, s_max = scores.min(), scores.max()
    rng = s_max - s_min
    if rng == 0:
        return np.zeros_like(scores, dtype=float)
    return (scores - s_min) / rng


def print_separator(title: str) -> None:
    print("\n" + "=" * 65)
    print(f"  {title}")
    print("=" * 65)


def score_stats(name: str, scores: np.ndarray, threshold: float = 0.5) -> dict:
    """Print and return summary statistics for a score array."""
    n_anom = int((scores >= threshold).sum())
    pct = n_anom / len(scores) * 100 if len(scores) > 0 else 0.0
    stats = {
        "n_anomalies": n_anom,
        "pct_anomalies": pct,
        "mean": float(np.mean(scores)),
        "std": float(np.std(scores)),
        "p25": float(np.percentile(scores, 25)),
        "p50": float(np.percentile(scores, 50)),
        "p75": float(np.percentile(scores, 75)),
        "p95": float(np.percentile(scores, 95)),
        "p99": float(np.percentile(scores, 99)),
    }
    print(f"  {name}:")
    print(f"    Anomalies (>={threshold:.2f}): {n_anom:,} ({pct:.2f}%)")
    print(f"    Mean={stats['mean']:.4f}  Std={stats['std']:.4f}")
    print(f"    P25={stats['p25']:.4f}  P50={stats['p50']:.4f}  "
          f"P75={stats['p75']:.4f}  P95={stats['p95']:.4f}  P99={stats['p99']:.4f}")
    return stats


def overlap_with_fraud(scores: np.ndarray, is_fraud: np.ndarray,
                       threshold: float = 0.5) -> int:
    """Count how many known-fraud apps are also flagged anomalous."""
    flagged = scores >= threshold
    both = int(((is_fraud == 1) & flagged).sum())
    total_fraud = int((is_fraud == 1).sum())
    total_flagged = int(flagged.sum())
    if total_flagged > 0:
        print(f"    Overlap with known fraud: {both}/{total_fraud} "
              f"({both/total_fraud*100:.1f}% recall)  |  "
              f"{both}/{total_flagged} flagged are true fraud "
              f"({both/total_flagged*100:.1f}% precision)")
    return both


# ═══════════════════════════════════════════════════════════════════════════════
#  1.  DATA LOADING
# ═══════════════════════════════════════════════════════════════════════════════

def load_data():
    LOG.info("Loading feature matrix and labels ...")
    if not FEATURE_PATH.exists():
        LOG.error(f"Feature matrix not found at {FEATURE_PATH}")
        sys.exit(1)
    if not LABELS_PATH.exists():
        LOG.error(f"Labels file not found at {LABELS_PATH}")
        sys.exit(1)

    features = pd.read_parquet(FEATURE_PATH)
    labels = pd.read_parquet(LABELS_PATH)

    LOG.info(f"  Features: {features.shape[0]:,} rows x {features.shape[1]} columns")
    LOG.info(f"  Labels:   {labels.shape[0]:,} rows")

    # Merge
    df = features.merge(labels[["application_id", "is_fraud"]], on="application_id", how="left")
    df["is_fraud"] = df["is_fraud"].fillna(0).astype(int)

    feature_cols = [c for c in features.columns if c != "application_id"]
    LOG.info(f"  Feature columns: {len(feature_cols)}")
    LOG.info(f"  Fraud distribution: {df['is_fraud'].value_counts().to_dict()}")

    return df, feature_cols


# ═══════════════════════════════════════════════════════════════════════════════
#  2.  HELPER: identify financial feature columns
# ═══════════════════════════════════════════════════════════════════════════════

def get_financial_cols(feature_cols: list) -> list:
    """Return the subset of feature columns that are financial in nature."""
    keywords = [
        "income", "assets", "liabilities", "net_worth",
        "debt_to_income", "assets_to_income",
    ]
    fin_cols = [c for c in feature_cols
                if any(kw in c.lower() for kw in keywords)]
    # Also include demographic outlier columns if present
    for extra in ["age_at_filing", "height_outlier", "weight_outlier"]:
        if extra in feature_cols and extra not in fin_cols:
            fin_cols.append(extra)
    return fin_cols


# ═══════════════════════════════════════════════════════════════════════════════
#  3A.  ISOLATION FOREST
# ═══════════════════════════════════════════════════════════════════════════════

def run_isolation_forest(X_scaled: np.ndarray, is_fraud: np.ndarray):
    """Train Isolation Forest on all data, return normalized anomaly scores."""
    print_separator("3A. Isolation Forest")
    t0 = time.time()

    iso = IsolationForest(
        n_estimators=300,
        max_samples=0.5,
        contamination=0.03,
        random_state=42,
        n_jobs=-1,
    )
    iso.fit(X_scaled)

    # decision_function: lower = more anomalous → invert for normalization
    raw_scores = -iso.decision_function(X_scaled)  # negate so higher = more anomalous
    if_scores = normalize_scores(raw_scores)

    dur = time.time() - t0
    LOG.info(f"  Isolation Forest trained in {dur:.1f}s")

    score_stats("IF scores", if_scores, threshold=0.5)
    overlap_with_fraud(if_scores, is_fraud, threshold=0.5)

    # Save model
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(iso, MODELS_DIR / "iso_forest.joblib")
    LOG.info(f"  Saved model to {MODELS_DIR / 'iso_forest.joblib'}")

    return iso, if_scores


# ═══════════════════════════════════════════════════════════════════════════════
#  3B.  AUTOENCODER (PyTorch)
# ═══════════════════════════════════════════════════════════════════════════════

def build_autoencoder(input_dim: int):
    """Build the AE architecture: D → 64 → 32 → 16 → 32 → 64 → D."""
    model = nn.Sequential(
        nn.Linear(input_dim, 64),
        nn.ReLU(),
        nn.Linear(64, 32),
        nn.ReLU(),
        nn.Linear(32, 16),
        nn.ReLU(),
        nn.Linear(16, 32),
        nn.ReLU(),
        nn.Linear(32, 64),
        nn.ReLU(),
        nn.Linear(64, input_dim),
    )
    return model


class Encoder(nn.Module):
    """Extract the encoder portion (first 6 layers: Linear→ReLU → Linear→ReLU → Linear→ReLU)
    to produce the 16-dim bottleneck embedding."""
    def __init__(self, full_ae: nn.Sequential):
        super().__init__()
        # Layers 0-5: input→64(ReLU)→32(ReLU)→16(ReLU)
        self.encoder = nn.Sequential(*list(full_ae.children())[:6])

    def forward(self, x):
        return self.encoder(x)


def run_autoencoder(X_scaled: np.ndarray, is_fraud: np.ndarray):
    """Train AE on clean (is_fraud=0) apps, score all apps by reconstruction error."""
    if not HAS_TORCH:
        return None, None, None

    print_separator("3B. Autoencoder (Semi-Supervised)")
    t0 = time.time()

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    LOG.info(f"  Device: {device}")

    D = X_scaled.shape[1]
    LOG.info(f"  Input dimension D = {D}")

    # Split clean data 80/20 for train/val
    clean_mask = is_fraud == 0
    X_clean = X_scaled[clean_mask]

    n_clean = len(X_clean)
    n_train = int(n_clean * 0.8)
    indices = np.random.RandomState(42).permutation(n_clean)
    X_train_ae = X_clean[indices[:n_train]]
    X_val_ae = X_clean[indices[n_train:]]

    LOG.info(f"  Clean train: {len(X_train_ae):,}  |  Clean val: {len(X_val_ae):,}")

    # DataLoaders
    train_ds = TensorDataset(torch.FloatTensor(X_train_ae))
    val_ds = TensorDataset(torch.FloatTensor(X_val_ae))
    train_loader = DataLoader(train_ds, batch_size=256, shuffle=True)
    val_loader = DataLoader(val_ds, batch_size=256, shuffle=False)

    # Build model
    model = build_autoencoder(D).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.0001, weight_decay=1e-5)
    criterion = nn.MSELoss()

    # Training loop with early stopping
    best_val_loss = float("inf")
    patience_counter = 0
    patience = 10
    best_state = None

    for epoch in range(1, 201):
        # --- Train ---
        model.train()
        train_loss = 0.0
        n_batches = 0
        for (batch_x,) in train_loader:
            batch_x = batch_x.to(device)
            recon = model(batch_x)
            loss = criterion(recon, batch_x)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            train_loss += loss.item()
            n_batches += 1
        train_loss /= max(n_batches, 1)

        # --- Validate ---
        model.eval()
        val_loss = 0.0
        n_val = 0
        with torch.no_grad():
            for (batch_x,) in val_loader:
                batch_x = batch_x.to(device)
                recon = model(batch_x)
                loss = criterion(recon, batch_x)
                val_loss += loss.item()
                n_val += 1
        val_loss /= max(n_val, 1)

        if epoch % 20 == 0 or epoch == 1:
            LOG.info(f"    Epoch {epoch:3d}  train_loss={train_loss:.6f}  val_loss={val_loss:.6f}")

        # Early stopping
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            patience_counter = 0
            best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
        else:
            patience_counter += 1
            if patience_counter >= patience:
                LOG.info(f"    Early stopping at epoch {epoch} (best val_loss={best_val_loss:.6f})")
                break

    # Restore best weights
    if best_state is not None:
        model.load_state_dict(best_state)
    model.to(device)
    model.eval()

    # Save model
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    torch.save(model.state_dict(), MODELS_DIR / "autoencoder.pt")
    LOG.info(f"  Saved model to {MODELS_DIR / 'autoencoder.pt'}")

    # --- Score ALL apps by per-sample MSE ---
    X_all_tensor = torch.FloatTensor(X_scaled).to(device)
    with torch.no_grad():
        recon_all = model(X_all_tensor)
    mse_per_sample = ((X_all_tensor - recon_all) ** 2).mean(dim=1).cpu().numpy()

    ae_scores = normalize_scores(mse_per_sample)

    # Threshold: 95th percentile of validation reconstruction error
    X_val_tensor = torch.FloatTensor(X_val_ae).to(device)
    with torch.no_grad():
        recon_val = model(X_val_tensor)
    val_mse = ((X_val_tensor - recon_val) ** 2).mean(dim=1).cpu().numpy()
    ae_threshold_raw = np.percentile(val_mse, 95)
    # Normalize threshold using same min/max as full scores
    s_min, s_max = mse_per_sample.min(), mse_per_sample.max()
    rng = s_max - s_min
    ae_threshold = (ae_threshold_raw - s_min) / rng if rng > 0 else 0.5

    dur = time.time() - t0
    LOG.info(f"  Autoencoder trained in {dur:.1f}s")
    LOG.info(f"  AE threshold (95th pct of val recon error, normalized): {ae_threshold:.4f}")

    score_stats("AE scores", ae_scores, threshold=ae_threshold)
    overlap_with_fraud(ae_scores, is_fraud, threshold=ae_threshold)

    # --- Extract encoder for hybrid ---
    encoder = Encoder(model).to(device)
    encoder.eval()

    return model, ae_scores, encoder


# ═══════════════════════════════════════════════════════════════════════════════
#  3C.  AUTOENCODER + ISOLATION FOREST HYBRID
# ═══════════════════════════════════════════════════════════════════════════════

def run_hybrid(encoder, ae_scores: np.ndarray, X_scaled: np.ndarray,
               is_fraud: np.ndarray):
    """Run IF on 16-dim AE latent embeddings, combine with AE scores."""
    if not HAS_TORCH or encoder is None:
        return None

    print_separator("3C. AE + Isolation Forest Hybrid")
    t0 = time.time()

    device = next(encoder.parameters()).device

    # Extract 16-dim latent embeddings
    X_tensor = torch.FloatTensor(X_scaled).to(device)
    with torch.no_grad():
        latent = encoder(X_tensor).cpu().numpy()
    LOG.info(f"  Latent embeddings shape: {latent.shape}")

    # Run new IF on latent space
    iso_latent = IsolationForest(
        n_estimators=300,
        max_samples=0.5,
        contamination=0.03,
        random_state=42,
        n_jobs=-1,
    )
    iso_latent.fit(latent)
    raw_latent_scores = -iso_latent.decision_function(latent)
    if_latent_scores = normalize_scores(raw_latent_scores)

    # Combine: hybrid_score = 0.6 * ae + 0.4 * if_latent
    hybrid_scores = 0.6 * ae_scores + 0.4 * if_latent_scores

    dur = time.time() - t0
    LOG.info(f"  Hybrid model computed in {dur:.1f}s")

    score_stats("Hybrid scores", hybrid_scores, threshold=0.5)
    overlap_with_fraud(hybrid_scores, is_fraud, threshold=0.5)

    return hybrid_scores


# ═══════════════════════════════════════════════════════════════════════════════
#  3D.  LOCAL OUTLIER FACTOR
# ═══════════════════════════════════════════════════════════════════════════════

def run_lof(X_scaled: np.ndarray, is_fraud: np.ndarray, feature_cols: list):
    """LOF on financial+demographic feature subset, trained on clean data."""
    print_separator("3D. Local Outlier Factor")
    t0 = time.time()

    fin_cols = get_financial_cols(feature_cols)
    if not fin_cols:
        LOG.warning("  No financial features found — skipping LOF")
        return np.zeros(len(X_scaled))

    fin_idx = [feature_cols.index(c) for c in fin_cols if c in feature_cols]
    LOG.info(f"  Financial feature subset ({len(fin_idx)} cols): {[feature_cols[i] for i in fin_idx]}")

    X_fin = X_scaled[:, fin_idx]
    clean_mask = is_fraud == 0
    X_fin_clean = X_fin[clean_mask]

    lof = LocalOutlierFactor(
        n_neighbors=20,
        contamination=0.03,
        novelty=True,
        n_jobs=-1,
    )
    lof.fit(X_fin_clean)

    # Score all data (novelty mode: decision_function available after fit)
    raw_scores = -lof.decision_function(X_fin)  # negate: higher = more anomalous
    lof_scores = normalize_scores(raw_scores)

    dur = time.time() - t0
    LOG.info(f"  LOF trained in {dur:.1f}s")

    score_stats("LOF scores", lof_scores, threshold=0.5)
    overlap_with_fraud(lof_scores, is_fraud, threshold=0.5)

    # Save model
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(lof, MODELS_DIR / "lof.joblib")
    LOG.info(f"  Saved model to {MODELS_DIR / 'lof.joblib'}")

    return lof_scores


# ═══════════════════════════════════════════════════════════════════════════════
#  3E.  COPOD (Copula-Based Outlier Detection)
# ═══════════════════════════════════════════════════════════════════════════════

def run_copod(X_scaled: np.ndarray, is_fraud: np.ndarray):
    """COPOD on all data."""
    if not HAS_PYOD:
        return np.zeros(len(X_scaled))

    print_separator("3E. COPOD (Copula-Based Outlier Detection)")
    t0 = time.time()

    copod = COPOD(contamination=0.03, n_jobs=-1)
    copod.fit(X_scaled)

    raw_scores = copod.decision_scores_
    copod_scores = normalize_scores(raw_scores)

    dur = time.time() - t0
    LOG.info(f"  COPOD trained in {dur:.1f}s")

    score_stats("COPOD scores", copod_scores, threshold=0.5)
    overlap_with_fraud(copod_scores, is_fraud, threshold=0.5)

    # Save model
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(copod, MODELS_DIR / "copod.joblib")
    LOG.info(f"  Saved model to {MODELS_DIR / 'copod.joblib'}")

    return copod_scores


# ═══════════════════════════════════════════════════════════════════════════════
#  3F.  MAHALANOBIS DISTANCE
# ═══════════════════════════════════════════════════════════════════════════════

def run_mahalanobis(df: pd.DataFrame, is_fraud: np.ndarray, feature_cols: list):
    """Robust Mahalanobis distance on financial features, trained on clean data."""
    print_separator("3F. Mahalanobis Distance")
    t0 = time.time()

    # Select financial columns from raw (unscaled) features for Mahalanobis
    fin_keywords = ["income", "assets", "liabilities", "net_worth",
                    "debt_to_income", "assets_to_income"]
    fin_cols = [c for c in feature_cols if any(kw in c.lower() for kw in fin_keywords)]

    if not fin_cols:
        LOG.warning("  No financial features found — skipping Mahalanobis")
        return np.zeros(len(df))

    LOG.info(f"  Financial features ({len(fin_cols)}): {fin_cols}")

    X_fin = df[fin_cols].values.astype(float)
    clean_mask = is_fraud == 0
    X_fin_clean = X_fin[clean_mask]

    # Robust covariance estimation
    try:
        mcd = MinCovDet(support_fraction=0.75, random_state=42)
        mcd.fit(X_fin_clean)
        LOG.info("  Using MinCovDet (robust covariance)")
    except Exception as e:
        LOG.warning(f"  MinCovDet failed ({e}) — falling back to EmpiricalCovariance")
        mcd = EmpiricalCovariance()
        mcd.fit(X_fin_clean)

    # Mahalanobis distance for all data
    mahal_dists = mcd.mahalanobis(X_fin)

    # Chi-squared threshold
    n_features = len(fin_cols)
    chi2_threshold = chi2.ppf(0.99, df=n_features)
    LOG.info(f"  Chi2 threshold (p=0.99, df={n_features}): {chi2_threshold:.4f}")

    mahal_scores = normalize_scores(mahal_dists)

    dur = time.time() - t0
    LOG.info(f"  Mahalanobis computed in {dur:.1f}s")

    # Use a normalized threshold for reporting
    n_above = int((mahal_dists >= chi2_threshold).sum())
    print(f"  Apps above chi2 threshold: {n_above:,} "
          f"({n_above / len(mahal_dists) * 100:.2f}%)")
    score_stats("Mahalanobis scores", mahal_scores, threshold=0.5)
    overlap_with_fraud(mahal_scores, is_fraud, threshold=0.5)

    return mahal_scores


# ═══════════════════════════════════════════════════════════════════════════════
#  3G.  BENFORD'S LAW ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════

def run_benford(df: pd.DataFrame, is_fraud: np.ndarray, feature_cols: list):
    """Benford's law conformity on income and assets columns."""
    print_separator("3G. Benford's Law Analysis")
    t0 = time.time()

    # Benford expected distribution for first digit 1-9
    BENFORD_EXPECTED = {
        d: np.log10(1 + 1 / d) for d in range(1, 10)
    }

    # Find income and assets columns
    target_cols = [c for c in feature_cols
                   if "income" in c.lower() or "assets" in c.lower()]

    if not target_cols:
        LOG.warning("  No income/assets columns found — skipping Benford")
        return np.zeros(len(df))

    LOG.info(f"  Applying Benford's Law to: {target_cols}")

    def first_digit(val):
        """Extract first non-zero digit from a numeric value."""
        try:
            v = abs(float(val))
            if v == 0:
                return 0
            s = f"{v:.0f}"
            for ch in s:
                if ch != "0" and ch != ".":
                    return int(ch)
            return 0
        except (ValueError, TypeError):
            return 0

    # Compute observed first-digit distribution across all target columns
    all_first_digits = []
    per_app_digits = {}  # application index -> list of first digits

    for col in target_cols:
        vals = df[col].values
        for i, v in enumerate(vals):
            fd = first_digit(v)
            if fd > 0:
                all_first_digits.append(fd)
                per_app_digits.setdefault(i, []).append(fd)

    if not all_first_digits:
        LOG.warning("  No valid first digits extracted — skipping Benford")
        return np.zeros(len(df))

    # Observed distribution
    observed_counts = np.zeros(10)
    for d in all_first_digits:
        observed_counts[d] += 1
    total_digits = observed_counts[1:].sum()
    observed_freq = {d: observed_counts[d] / total_digits if total_digits > 0 else 0
                     for d in range(1, 10)}

    LOG.info("  Overall first-digit distribution:")
    LOG.info("  Digit  Expected  Observed")
    for d in range(1, 10):
        LOG.info(f"    {d}     {BENFORD_EXPECTED[d]:.4f}    {observed_freq[d]:.4f}")

    # Find the rarest Benford bin (the digit with the largest overrepresentation
    # relative to expected, indicating possible fabrication)
    # Deviation = |observed - expected|
    deviations = {d: abs(observed_freq[d] - BENFORD_EXPECTED[d]) for d in range(1, 10)}
    rarest_digit = max(deviations, key=deviations.get)
    LOG.info(f"  Rarest Benford bin (most deviating digit): {rarest_digit} "
             f"(deviation={deviations[rarest_digit]:.4f})")

    # Per-application: compute deviation from expected + flag if first digit
    # is in the rarest bin
    benford_flags = np.zeros(len(df), dtype=int)

    for i in range(len(df)):
        digits = per_app_digits.get(i, [])
        if not digits:
            continue

        # Flag: does this application have a first digit in the rarest bin?
        has_rarest = any(d == rarest_digit for d in digits)

        # Compute per-app deviation (KL-like):
        app_counts = np.zeros(10)
        for d in digits:
            app_counts[d] += 1
        n_digits = app_counts[1:].sum()
        if n_digits > 0:
            app_freq = {d: app_counts[d] / n_digits for d in range(1, 10)}
            deviation = sum(abs(app_freq[d] - BENFORD_EXPECTED[d]) for d in range(1, 10))
            # Flag if deviation is high AND has rarest digit
            # Threshold: deviation > 0.5 (significant departure from Benford)
            if has_rarest and deviation > 0.5:
                benford_flags[i] = 1

    dur = time.time() - t0
    n_flagged = int(benford_flags.sum())
    LOG.info(f"  Benford flags: {n_flagged:,} ({n_flagged / len(df) * 100:.2f}%)")
    LOG.info(f"  Benford analysis completed in {dur:.1f}s")

    # Overlap
    fraud_flagged = int(((is_fraud == 1) & (benford_flags == 1)).sum())
    total_fraud = int((is_fraud == 1).sum())
    print(f"  Overlap with known fraud: {fraud_flagged}/{total_fraud}")

    return benford_flags


# ═══════════════════════════════════════════════════════════════════════════════
#  3H.  ONE-CLASS SVM (E8)
# ═══════════════════════════════════════════════════════════════════════════════

def run_ocsvm(X_scaled: np.ndarray, is_fraud: np.ndarray):
    """One-Class SVM trained on clean applications only.

    Uses RBF kernel with nu=0.03 (expected contamination rate).
    Trains on is_fraud==0 apps, scores all apps.
    """
    from sklearn.svm import OneClassSVM

    print_separator("3H — One-Class SVM")
    t0 = time.time()

    # Train on clean data only
    clean_mask = is_fraud == 0
    X_clean = X_scaled[clean_mask]

    # OC-SVM is O(n^2) — subsample if too large
    max_train = 50000
    if len(X_clean) > max_train:
        LOG.info(f"  Subsampling clean data: {len(X_clean):,} → {max_train:,}")
        rng = np.random.RandomState(42)
        idx = rng.choice(len(X_clean), max_train, replace=False)
        X_train = X_clean[idx]
    else:
        X_train = X_clean

    LOG.info(f"  Training OC-SVM on {len(X_train):,} clean samples ...")
    ocsvm = OneClassSVM(
        kernel="rbf",
        nu=0.03,
        gamma="scale",
    )
    ocsvm.fit(X_train)

    # Score all applications
    # decision_function: positive = inlier, negative = outlier
    raw_scores = ocsvm.decision_function(X_scaled)
    # Invert and normalize: higher = more anomalous
    ocsvm_scores = normalize_scores(-raw_scores)

    dur = time.time() - t0
    LOG.info(f"  OC-SVM trained and scored in {dur:.1f}s")

    # Save model
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(ocsvm, MODELS_DIR / "ocsvm.joblib")
    LOG.info(f"  Saved {MODELS_DIR / 'ocsvm.joblib'}")

    # Stats
    stats = score_stats("OC-SVM", ocsvm_scores, threshold=0.5)

    # Overlap with known fraud
    fraud_flagged = int(((is_fraud == 1) & (ocsvm_scores >= 0.5)).sum())
    total_fraud = int((is_fraud == 1).sum())
    print(f"  Overlap with known fraud: {fraud_flagged}/{total_fraud}")

    return ocsvm_scores


# ═══════════════════════════════════════════════════════════════════════════════
#  4.  NOVEL ANOMALY DETECTION
# ═══════════════════════════════════════════════════════════════════════════════

def find_novel_anomalies(scores_df: pd.DataFrame, is_fraud: np.ndarray):
    """Find apps flagged as anomalous by >= 2 methods but NOT in fraud manifest."""
    print_separator("Novel Anomaly Detection")

    # Define thresholds: continuous scores > 0.5, benford is already binary
    method_cols = {
        "if_score": 0.5,
        "ae_score": 0.5,
        "hybrid_score": 0.5,
        "lof_score": 0.5,
        "copod_score": 0.5,
        "mahalanobis_score": 0.5,
        "benford_flag": 0.5,  # already 0/1
        "ocsvm_score": 0.5,
    }

    # Count how many methods flag each application
    detection_count = np.zeros(len(scores_df), dtype=int)
    method_names = []

    for col, thresh in method_cols.items():
        if col in scores_df.columns:
            flagged = (scores_df[col].values >= thresh).astype(int)
            detection_count += flagged
            method_names.append(col)

    scores_df["detection_count"] = detection_count

    # Build methods string per application
    methods_list = []
    for i in range(len(scores_df)):
        flagged_methods = []
        for col, thresh in method_cols.items():
            if col in scores_df.columns and scores_df[col].iloc[i] >= thresh:
                flagged_methods.append(col.replace("_score", "").replace("_flag", ""))
        methods_list.append(",".join(flagged_methods))
    scores_df["methods"] = methods_list

    # Novel: detected by >= 2 methods AND is_fraud == 0
    novel_mask = (detection_count >= 2) & (is_fraud == 0)
    novel_df = scores_df.loc[novel_mask, ["application_id", "detection_count", "methods"]].copy()
    novel_df = novel_df.sort_values("detection_count", ascending=False).reset_index(drop=True)

    LOG.info(f"  Novel anomalies (>= 2 methods, not in fraud manifest): {len(novel_df):,}")
    if len(novel_df) > 0:
        print(f"  Detection count distribution:")
        print(f"    {novel_df['detection_count'].value_counts().sort_index().to_dict()}")
        print(f"  Top 10 novel anomalies:")
        print(novel_df.head(10).to_string(index=False))

    return novel_df


# ═══════════════════════════════════════════════════════════════════════════════
#  5.  MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    t0_total = time.time()
    print("=" * 65)
    print("  I-485 FRAUD DETECTION — UNSUPERVISED ANOMALY MODELS")
    print("=" * 65)

    # ── Load data ─────────────────────────────────────────────────────────────
    df, feature_cols = load_data()
    application_ids = df["application_id"].values
    is_fraud = df["is_fraud"].values
    X_raw = df[feature_cols].values.astype(float)

    # ── Scale features ────────────────────────────────────────────────────────
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_raw)
    LOG.info(f"Features scaled: {X_scaled.shape}")

    # ── 3A: Isolation Forest ──────────────────────────────────────────────────
    iso_model, if_scores = run_isolation_forest(X_scaled, is_fraud)

    # ── 3B: Autoencoder ───────────────────────────────────────────────────────
    ae_model, ae_scores, encoder = run_autoencoder(X_scaled, is_fraud)
    if ae_scores is None:
        ae_scores = np.zeros(len(df))

    # ── 3C: Hybrid ────────────────────────────────────────────────────────────
    hybrid_scores = run_hybrid(encoder, ae_scores, X_scaled, is_fraud)
    if hybrid_scores is None:
        hybrid_scores = np.zeros(len(df))

    # ── 3D: LOF ───────────────────────────────────────────────────────────────
    lof_scores = run_lof(X_scaled, is_fraud, feature_cols)

    # ── 3E: COPOD ─────────────────────────────────────────────────────────────
    copod_scores = run_copod(X_scaled, is_fraud)

    # ── 3F: Mahalanobis ───────────────────────────────────────────────────────
    mahal_scores = run_mahalanobis(df, is_fraud, feature_cols)

    # ── 3G: Benford ───────────────────────────────────────────────────────────
    benford_flags = run_benford(df, is_fraud, feature_cols)

    # ── 3H: One-Class SVM (E8) ────────────────────────────────────────────────
    ocsvm_scores = run_ocsvm(X_scaled, is_fraud)

    # ── Assemble output ───────────────────────────────────────────────────────
    print_separator("Output Assembly")

    scores_df = pd.DataFrame({
        "application_id": application_ids,
        "if_score": if_scores,
        "ae_score": ae_scores,
        "hybrid_score": hybrid_scores,
        "lof_score": lof_scores,
        "copod_score": copod_scores,
        "mahalanobis_score": mahal_scores,
        "benford_flag": benford_flags,
        "ocsvm_score": ocsvm_scores,
    })

    # ── Save unsupervised scores ──────────────────────────────────────────────
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    scores_path = RESULTS_DIR / "unsupervised_scores.parquet"
    scores_df.to_parquet(scores_path, index=False)
    LOG.info(f"  Saved {scores_path}  ({len(scores_df):,} rows)")

    # ── Find novel anomalies ──────────────────────────────────────────────────
    novel_df = find_novel_anomalies(scores_df, is_fraud)
    novel_path = RESULTS_DIR / "novel_anomalies.parquet"
    novel_df.to_parquet(novel_path, index=False)
    LOG.info(f"  Saved {novel_path}  ({len(novel_df):,} rows)")

    # ── Final Summary ─────────────────────────────────────────────────────────
    print_separator("PIPELINE SUMMARY")

    total_fraud = int((is_fraud == 1).sum())
    total_clean = int((is_fraud == 0).sum())
    dur = time.time() - t0_total

    print(f"  Total applications:   {len(df):,}")
    print(f"  Known fraud (label):  {total_fraud:,}")
    print(f"  Clean (label):        {total_clean:,}")
    print(f"  Feature dimensions:   {len(feature_cols)}")
    print(f"  Duration:             {dur:.1f}s ({dur/60:.1f} min)")
    print()

    # Per-model summary
    model_names = ["if_score", "ae_score", "hybrid_score", "lof_score",
                   "copod_score", "mahalanobis_score", "benford_flag",
                   "ocsvm_score"]
    model_labels = ["Isolation Forest", "Autoencoder", "AE+IF Hybrid", "LOF",
                    "COPOD", "Mahalanobis", "Benford's Law", "One-Class SVM"]

    print(f"  {'Model':<20s}  {'Anomalies':>10s}  {'Fraud Overlap':>14s}")
    print(f"  {'-'*20}  {'-'*10}  {'-'*14}")
    for col, label in zip(model_names, model_labels):
        vals = scores_df[col].values
        thresh = 0.5
        n_anom = int((vals >= thresh).sum())
        fraud_overlap = int(((is_fraud == 1) & (vals >= thresh)).sum())
        print(f"  {label:<20s}  {n_anom:>10,}  {fraud_overlap:>14,}")

    print(f"\n  Novel anomalies (>= 2 methods, not fraud): {len(novel_df):,}")
    print()

    # Score distribution table
    print(f"  {'Score Column':<20s}  {'Mean':>8s}  {'Std':>8s}  {'P50':>8s}  {'P95':>8s}  {'P99':>8s}")
    print(f"  {'-'*20}  {'-'*8}  {'-'*8}  {'-'*8}  {'-'*8}  {'-'*8}")
    for col in model_names:
        vals = scores_df[col].values
        print(f"  {col:<20s}  {np.mean(vals):>8.4f}  {np.std(vals):>8.4f}  "
              f"{np.percentile(vals, 50):>8.4f}  {np.percentile(vals, 95):>8.4f}  "
              f"{np.percentile(vals, 99):>8.4f}")

    print()
    print("  Output files:")
    print(f"    {scores_path}")
    print(f"    {novel_path}")
    print(f"    {MODELS_DIR / 'autoencoder.pt'}")
    print(f"    {MODELS_DIR / 'iso_forest.joblib'}")
    print(f"    {MODELS_DIR / 'lof.joblib'}")
    print(f"    {MODELS_DIR / 'copod.joblib'}")
    print(f"    {MODELS_DIR / 'ocsvm.joblib'}")
    print()
    print("  Done.")


if __name__ == "__main__":
    main()
