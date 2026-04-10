import os
import json
import glob
from typing import List, Dict, Any

import pandas as pd
import numpy as np


def load_pbp_supabase() -> pd.DataFrame:
    """Load PBP data from Supabase pwhl_pbp table."""
    from supabase_utils import get_supabase_client

    client = get_supabase_client()
    # Fetch all rows; paginate in chunks of 1000 to avoid API limits
    all_rows: List[Dict[str, Any]] = []
    page_size = 1000
    offset = 0
    while True:
        resp = (
            client.table("pwhl_pbp")
            .select("*")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = resp.data or []
        all_rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    if not all_rows:
        raise SystemExit("No PBP rows found in Supabase")
    return pd.DataFrame(all_rows)


def load_pbp_csvs(root: str) -> pd.DataFrame:
    folder = os.path.join(root, 'Data', 'Play-by-Play')
    paths = sorted(glob.glob(os.path.join(folder, '*_shots.csv')))
    if not paths:
        raise SystemExit(f"No PBP CSVs found in {folder}")
    frames: List[pd.DataFrame] = []
    for p in paths:
        try:
            df = pd.read_csv(p)
            if df.empty:
                continue
            df['__source_file'] = os.path.basename(p)
            frames.append(df)
        except Exception:
            # Skip corrupt files
            continue
    if not frames:
        raise SystemExit("No readable PBP CSVs found")
    # Drop all-NA columns before concat to avoid FutureWarning
    frames = [f.dropna(axis=1, how='all') for f in frames]
    return pd.concat(frames, ignore_index=True)


def map_strength_to_state(s: str) -> str:
    if not isinstance(s, str) or 'v' not in s:
        return 'EV'
    parts = s.lower().split('v')
    try:
        a = int(parts[0])
        b = int(parts[1])
    except Exception:
        return 'EV'
    if a == b:
        return 'EV'
    return 'PP' if a > b else 'SH'


def prep_dataset(df: pd.DataFrame) -> pd.DataFrame:
    # Keep only Shot and Goal events
    df = df.copy()
    df['event'] = df['event'].astype(str)
    df = df[df['event'].isin(['Shot', 'Goal'])]

    # y: Goal -> 1, Shot -> 0
    df['y'] = (df['event'] == 'Goal').astype(int)

    # StrengthState from `strength` like '5v4' relative to event team
    if 'strength' not in df.columns:
        raise SystemExit("Input CSVs missing 'strength' column")
    df['StrengthState'] = df['strength'].apply(map_strength_to_state).astype('category')

    # ScoreState clamp to [-2, 2]
    def clamp_score(v: Any) -> str:
        try:
            i = int(v)
        except Exception:
            i = 0
        if i < -2:
            i = -2
        if i > 2:
            i = 2
        return str(i)
    if 'ScoreState' not in df.columns:
        # tolerate missing; default to 0
        df['ScoreState'] = '0'
    df['ScoreState'] = df['ScoreState'].apply(clamp_score).astype('category')

    # BoxID mapping: O* stays as-is; N* or D* (or missing) -> 'N_or_D'
    def map_box(z: Any) -> str:
        s = str(z) if z is not None else ''
        if s.startswith('O'):
            return s
        return 'N_or_D'
    if 'BoxID' not in df.columns:
        # attempt to derive from zone columns if present; else default
        df['BoxID'] = 'N_or_D'
    df['BoxID'] = df['BoxID'].apply(map_box).astype('category')

    # Keep only needed columns
    keep = ['y', 'StrengthState', 'ScoreState', 'BoxID']
    return df[keep].dropna()


def build_logit(df: pd.DataFrame) -> Dict[str, Any]:
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import log_loss, roc_auc_score

    # One-hot encode categorical features with a baseline (drop_first=True)
    X = pd.get_dummies(
        df[['StrengthState', 'ScoreState', 'BoxID']],
        columns=['StrengthState', 'ScoreState', 'BoxID'],
        drop_first=False,  # no intercept => keep all levels
        dtype=float,
    )
    y = df['y'].astype(int).values

    # Fit logistic regression
    clf = LogisticRegression(max_iter=1000, solver='lbfgs', fit_intercept=False)
    clf.fit(X.values, y)

    coef = clf.coef_[0].tolist()
    intercept = 0.0  # not fitted
    features = list(X.columns)

    # Build metadata with category baselines so we can score consistently later
    meta: Dict[str, Any] = {
        'intercept': intercept,
        'features': features,
        'coefficients': {feat: float(w) for feat, w in zip(features, coef)},
        'baselines': {},  # not used (no intercept)
        'levels': {},
        'options': {
            'fit_intercept': False,
            'drop_first': False,
            'solver': 'lbfgs',
            'max_iter': 1000,
        }
    }

    # Recover baselines as the first category (sorted) that was dropped
    for col in ['StrengthState', 'ScoreState', 'BoxID']:
        cats = sorted(df[col].cat.categories.tolist())
        meta['levels'][col] = cats
        # No explicit baseline used (no intercept). Keep None for clarity.
        meta['baselines'][col] = None

    # Metrics on training set (apparent performance)
    y_prob = clf.predict_proba(X.values)[:, 1]
    try:
        auc = float(roc_auc_score(y, y_prob))
    except Exception:
        auc = float('nan')
    try:
        ll = float(log_loss(y, y_prob))
    except Exception:
        ll = float('nan')

    meta_metrics = {
        'log_loss': ll,
        'auc': auc,
    }

    return {
        'model': meta,
        'sklearn_version': '1.x',
        'n_rows': int(df.shape[0]),
        'class_balance': {
            'shots': int((df['y'] == 0).sum()),
            'goals': int((df['y'] == 1).sum()),
        },
        'metrics': meta_metrics,
    }


def save_outputs(root: str, meta: Dict[str, Any]):
    out_dir = os.path.join(root, 'models')
    os.makedirs(out_dir, exist_ok=True)
    # JSON model
    with open(os.path.join(out_dir, 'xg_model.json'), 'w', encoding='utf-8') as f:
        json.dump(meta, f, indent=2)
    # Coefficients CSV
    rows = [{'feature': 'intercept', 'coefficient': meta['model']['intercept']}]
    rows += [{'feature': k, 'coefficient': v} for k, v in meta['model']['coefficients'].items()]
    pd.DataFrame(rows).to_csv(os.path.join(out_dir, 'xg_model_coefficients.csv'), index=False)


def main():
    repo_root = os.path.dirname(os.path.abspath(__file__))

    # Try Supabase first, fall back to local CSV files
    try:
        print('Loading Play-by-Play from Supabase…')
        df = load_pbp_supabase()
        print(f"Loaded {len(df):,} rows from Supabase")
    except Exception as e:
        print(f"Supabase unavailable ({e}), falling back to CSV files…")
        df = load_pbp_csvs(repo_root)
        print(f"Loaded {len(df):,} rows from PBP files")

    print('Preparing dataset (filtering & feature engineering)…')
    ds = prep_dataset(df)
    print(f"Training rows: {len(ds):,} (goals={int((ds['y']==1).sum()):,}, shots={int((ds['y']==0).sum()):,})")
    print('Fitting logistic regression…')
    meta = build_logit(ds)
    print('Saving model outputs…')
    save_outputs(repo_root, meta)
    print('Done. Files: models/xg_model.json, models/xg_model_coefficients.csv')


if __name__ == '__main__':
    main()
