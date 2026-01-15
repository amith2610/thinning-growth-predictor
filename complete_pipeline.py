#!/usr/bin/env python3
"""
INTEGRATED FOREST THINNING AND GROWTH PROJECTION PIPELINE

Combines:
1. Complete thinning analysis tool (all 6 primary + 5 secondary strategies)
2. PTAEDA4 growth model (with full thinning response)

"""

import sys
import os
import yaml
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from functools import lru_cache
from tqdm import tqdm
from datetime import datetime
import json
import warnings
from openpyxl import load_workbook, Workbook

warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURATION LOADER
# ============================================================================

def load_configuration(config_file):
    """Load and validate configuration from YAML file"""
    
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Configuration file not found: {config_file}")
    
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    
    # Validate required sections
    required_sections = ['input', 'primary_thinning', 'stand_parameters', 'output']
    for section in required_sections:
        if section not in config:
            raise ValueError(f"Missing required configuration section: {section}")
    
    return config


# ============================================================================
# STRATEGY NAME BUILDER
# ============================================================================

def build_strategy_name(config):
    """
    Build a complete strategy name from configuration.
    
    Examples:
        "4-row_start5"
        "3-row_start2_Thin from Below_0.25"
        "5-row_start3_Thin by CI1 (Distance-Dependent Competition)_0.15"
    """
    primary_config = config['primary_thinning']
    primary_strategy = primary_config['strategy']
    start_row = primary_config['start_row']
    
    # Start with primary strategy
    strategy_name = f"{primary_strategy}_start{start_row}"
    
    # Add secondary if enabled
    secondary_config = config.get('secondary_thinning', {})
    if secondary_config.get('enabled', False):
        sec_strategy = secondary_config.get('strategy', 'Unknown')
        removal_frac = secondary_config.get('removal_fraction', 0.0)
        strategy_name += f"_{sec_strategy}_{removal_frac:.2f}"
    
    return strategy_name


# ============================================================================
# EXCEL EXPORT FUNCTIONS
# ============================================================================

def calculate_total_volume(dbh_values, height_values):
    """
    Calculate total stand volume using Tasissa et al. formula.
    
    V_ob = 0.25663 + 0.00239 × D² × H
    
    Args:
        dbh_values: Array of DBH values (inches)
        height_values: Array of height values (feet)
    
    Returns:
        Total volume (cubic feet)
    """
    volumes = 0.25663 + 0.00239 * (dbh_values ** 2) * height_values
    return volumes.sum()


def export_to_excel(strategy_name, mean_dbh, mean_height, total_volume, volume_after_thinning, excel_path, verbose=True):
    """
    Export metrics to Excel file for comparison analysis.
    
    Args:
        strategy_name: Complete strategy identifier
        mean_dbh: Mean DBH at final year (inches)
        mean_height: Mean height at final year (feet)
        total_volume: Total stand volume at final year (cubic feet)
        volume_after_thinning: Total stand volume immediately after thinning (cubic feet)
        excel_path: Path to Excel file
        verbose: Print status messages
    """
    
    # Calculate growth in volume
    growth_in_volume = total_volume - volume_after_thinning
    
    # Create new row data
    new_row = {
        'Strategy': strategy_name,
        'Mean_DBH': mean_dbh,
        'Mean_Height': mean_height,
        'Total_Volume': total_volume,
        'Volume_After_Thinning': volume_after_thinning,
        'Growth_in_Volume': growth_in_volume
    }
    
    # Check if file exists
    if os.path.exists(excel_path):
        try:
            # Load existing file
            df = pd.read_excel(excel_path)
            
            # Check if file has correct columns
            required_cols = ['Strategy', 'Mean_DBH', 'Mean_Height', 'Total_Volume', 'Volume_After_Thinning', 'Growth_in_Volume']
            if not all(col in df.columns for col in required_cols):
                # File exists but has wrong structure - recreate it
                if verbose:
                    print(f"   ⚠️  Existing file has incompatible structure. Creating new file...")
                df = pd.DataFrame([new_row])
            else:
                # Check if strategy already exists
                if strategy_name in df['Strategy'].values:
                    # Update existing row
                    df.loc[df['Strategy'] == strategy_name, ['Mean_DBH', 'Mean_Height', 'Total_Volume', 'Volume_After_Thinning', 'Growth_in_Volume']] = [mean_dbh, mean_height, total_volume, volume_after_thinning, growth_in_volume]
                    if verbose:
                        print(f"   ⚠️  Updated existing strategy: {strategy_name}")
                else:
                    # Append new row
                    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                    if verbose:
                        print(f"   ✅ Added new strategy: {strategy_name}")
        except Exception as e:
            # If any error reading the file, recreate it
            if verbose:
                print(f"   ⚠️  Could not read existing file ({str(e)}). Creating new file...")
            df = pd.DataFrame([new_row])
    else:
        # Create new file
        df = pd.DataFrame([new_row])
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(excel_path), exist_ok=True)
        if verbose:
            print(f"   ✅ Created new Excel file: {excel_path}")
            print(f"   ✅ Added new strategy: {strategy_name}")
    
    # Save to Excel
    df.to_excel(excel_path, index=False)
    
    if verbose:
        print(f"\n📊 Metrics exported to Excel:")
        print(f"   Strategy: {strategy_name}")
        print(f"   Mean DBH: {mean_dbh:.2f} inches")
        print(f"   Mean Height: {mean_height:.2f} feet")
        print(f"   Total Volume (Final): {total_volume:.2f} cubic feet")
        print(f"   Volume After Thinning: {volume_after_thinning:.2f} cubic feet")
        print(f"   Growth in Volume: {growth_in_volume:.2f} cubic feet")
        print(f"   File: {excel_path}")



# ============================================================================
# THINNING TOOL - CORE FUNCTIONS (PRESERVED EXACTLY)
# ============================================================================

def load_stand_data(filepath, columns):
    """Load and prepare stand data from CSV file"""
    df = pd.read_csv(filepath)
    
    # Validate columns exist
    for col_name, col_value in columns.items():
        if col_value not in df.columns:
            raise ValueError(f"Column '{col_value}' not found in data file")
    
    # Convert to numeric
    for col in columns.values():
        df[col] = pd.to_numeric(df[col], errors="coerce")
    
    # Add status column
    df["status"] = np.where(df[columns['height']].notna() & (df[columns['height']] > 0), "Alive", "Dead")
    
    # Order trees within rows
    df = order_within_rows(df, columns)
    
    return df


def order_within_rows(df, columns):
    """Order trees within each row - PRESERVED EXACTLY"""
    d = df.copy()
    row_col = columns['row']
    xcol = columns['x_coord']
    ycol = columns['y_coord']
    
    axis_choice = {}
    
    for r, g in d.groupby(row_col, sort=False):
        vx = float(g[xcol].var()) if len(g) > 1 else 0.0
        vy = float(g[ycol].var()) if len(g) > 1 else 0.0
        axis_choice[r] = xcol if vx >= vy else ycol

    def _sort_group(g):
        r = g.name
        axis = axis_choice.get(r, xcol)
        other = ycol if axis == xcol else xcol
        return g.sort_values([axis, other], kind="mergesort")

    d = d.groupby(row_col, group_keys=False, sort=False).apply(_sort_group)
    d["tree_idx_in_row"] = d.groupby(row_col).cumcount() + 1
    return d


def ordered_rows(df, row_col):
    """Get sorted list of unique row numbers - PRESERVED EXACTLY"""
    s = df[row_col].dropna()
    if s.empty:
        return []
    rows = sorted(pd.unique(s.astype(float)))
    rows = [int(r) if float(r).is_integer() else float(r) for r in rows]
    return rows


def row_pos_map(df, row_col):
    """Create mapping from row number to position index - PRESERVED EXACTLY"""
    rows = ordered_rows(df, row_col)
    return {r: i for i, r in enumerate(rows)}, rows


# ============================================================================
# PRIMARY THINNING STRATEGIES (PRESERVED EXACTLY)
# ============================================================================

def k_row_thinning(df, k, start_row, columns):
    """Apply k-row thinning pattern - PRESERVED EXACTLY"""
    assert k >= 2 and 1 <= start_row <= k
    
    d = df.copy()
    d["thin_decision"] = "Dead (ignored)"
    alive = d["status"].eq("Alive")
    
    row_col = columns['row']
    mp, rows = row_pos_map(d, row_col)
    pos = d[row_col].map(mp)
    start0 = start_row - 1
    rows_to_thin = ((pos - start0) % k == 0)
    
    d.loc[alive & rows_to_thin, "thin_decision"] = "Thin"
    d.loc[alive & ~rows_to_thin, "thin_decision"] = "Keep"
    return d


def variable_row_thinning(df, cut_rows, columns):
    """Apply variable row thinning - PRESERVED EXACTLY"""
    d = df.copy()
    d["thin_decision"] = "Dead (ignored)"
    alive = d["status"].eq("Alive")
    row_col = columns['row']
    in_cut = d[row_col].isin(cut_rows)
    d.loc[alive & in_cut, "thin_decision"] = "Thin"
    d.loc[alive & ~in_cut, "thin_decision"] = "Keep"
    return d


def _row_q4_volume_by_row(df, columns):
    """Calculate Q4 volume by row - PRESERVED EXACTLY"""
    metric = columns['dbh']
    vol_col = columns['volume']
    row_col = columns['row']
    status_col = 'status'
    
    alive = df[df[status_col] == "Alive"].copy()
    if alive.empty:
        raise ValueError("No Alive trees.")
    q3 = float(alive[metric].quantile(0.75))
    rows = ordered_rows(df, row_col)
    q4_rows = alive.loc[alive[metric] >= q3, [row_col, vol_col]]
    q4_vol_by_row = q4_rows.groupby(row_col)[vol_col].sum().reindex(rows, fill_value=0.0)
    return rows, q3, q4_vol_by_row.values


def _best_sequence_from_start_q4vol_with_min_gap(rows, q4_vols, start_idx, target_cuts, min_gap=3, max_gap=5):
    """DP to find optimal variable thinning sequence - PRESERVED EXACTLY"""
    N = len(rows)
    if target_cuts <= 0 or start_idx < 0 or start_idx >= N:
        return None
    if start_idx + min_gap * (target_cuts - 1) > N - 1:
        return None

    @lru_cache(maxsize=None)
    def dp(last_idx, selected):
        if selected == target_cuts:
            return (0.0, ())
        remaining = target_cuts - selected
        if last_idx + min_gap * remaining > N - 1:
            return None
        best = None
        cand = []
        
        for step in range(min_gap, max_gap + 1):
            nxt = last_idx + step
            if nxt <= N - 1:
                cand.append((float(q4_vols[nxt]), step, nxt))
        
        cand.sort(key=lambda x: (x[0], x[1], x[2]))
        
        for q4v, step, nxt in cand:
            rem_after = target_cuts - (selected + 1)
            if nxt + min_gap * rem_after > N - 1:
                continue
            sub = dp(nxt, selected + 1)
            if sub is None:
                continue
            sub_q4, sub_path = sub
            cand_val = (q4v + sub_q4, (nxt,) + sub_path)
            if best is None or (cand_val[0] < best[0]) or (cand_val[0] == best[0] and cand_val[1] < best[1]):
                best = cand_val
        return best

    start_cost = float(q4_vols[start_idx])
    sub = dp(start_idx, 1)
    if sub is None:
        return None
    sub_q4, sub_path = sub
    return (start_cost + sub_q4, (start_idx,) + sub_path)


def choose_variable_cut_rows(df, target_cuts, min_gap, max_gap, columns):
    """Choose rows to cut for variable thinning - PRESERVED EXACTLY"""
    rows, q3, q4_vols = _row_q4_volume_by_row(df, columns)
    N = len(rows)
    if N == 0:
        return []
    max_start_idx = min(5, N) - 1
    feasible = [s for s in range(0, max_start_idx + 1) if s + min_gap * (target_cuts - 1) <= N - 1]
    if not feasible:
        raise ValueError(f"Infeasible start with min_gap={min_gap}.")
    
    best_total = None
    best_path = None
    best_start = None
    for s in feasible:
        res = _best_sequence_from_start_q4vol_with_min_gap(rows, q4_vols, s, target_cuts, 
                                                            min_gap=min_gap, max_gap=max_gap)
        if res is None:
            continue
        tot, path = res
        if (best_total is None) or (tot < best_total) or (tot == best_total and s < best_start):
            best_total = tot
            best_path = path
            best_start = s
    if best_path is None:
        raise ValueError(f"No feasible sequence with min_gap={min_gap}, max_gap={max_gap}.")
    return [rows[i] for i in best_path]


def apply_variable_thinning(df, variant, columns):
    """Apply variable thinning strategy - PRESERVED EXACTLY"""
    
    row_col = columns['row']
    rows_sorted = ordered_rows(df, row_col)
    R = len(rows_sorted)
    
    variants_config = {
        "3_row_eqv": {"target_cuts": R // 3, "min_gap": 2, "max_gap": 4},
        "4_row_eqv": {"target_cuts": R // 4, "min_gap": 3, "max_gap": 5},
        "5_row_eqv": {"target_cuts": R // 5, "min_gap": 3, "max_gap": 4}
    }
    
    config = variants_config[variant]
    
    cut_rows = choose_variable_cut_rows(df, config["target_cuts"], 
                                        config["min_gap"], config["max_gap"], columns)
    
    return variable_row_thinning(df, cut_rows, columns), cut_rows


# ============================================================================
# SECONDARY THINNING STRATEGIES (PRESERVED EXACTLY)
# ============================================================================

def apply_secondary_thin_from_below(df_after_primary, fraction, columns):
    """
    Thin from Below - Remove smallest trees from residual stand
    PRESERVED EXACTLY from original
    """
    d = df_after_primary.copy()
    alive = d['status'].eq('Alive')
    dbh_col = columns['dbh']
    
    # Get all kept trees after primary
    all_kept = d.loc[alive & d['thin_decision'].eq('Keep')]
    total_kept = len(all_kept)
    total_budget = int(np.floor(total_kept * fraction))
    
    if total_budget <= 0:
        return d, {'n_removed': 0}
    
    # Sort by DBH (smallest first) and take bottom X%
    all_eligible = all_kept[[dbh_col]].copy()
    all_eligible_sorted = all_eligible.sort_values(dbh_col, ascending=True)
    trees_to_remove = all_eligible_sorted.head(total_budget)
    
    d.loc[trees_to_remove.index, 'thin_decision'] = 'Thin'
    
    return d, {'n_removed': len(trees_to_remove)}


def apply_secondary_thin_from_above_neighbors(df_after_primary, removal_fraction, 
                                               anchor_fraction, columns):
    """
    Thin from Above-1 (Neighbors) - Remove competitors near anchors
    PRESERVED EXACTLY from original
    """
    d0 = df_after_primary.copy()
    alive = d0['status'].eq('Alive')
    row_col = columns['row']
    dbh_col = columns['dbh']
    
    # Get all residual rows
    residual_rows = sorted(d0.loc[alive & d0['thin_decision'].eq('Keep'), row_col].unique())
    
    if len(residual_rows) == 0:
        return d0, {
            'residual_rows': [], 
            'per_row_quota': {}, 
            'per_row_removed': {}, 
            'anchors_per_row': {}
        }
    
    per_row_quota = {}
    per_row_eligible_idx = {}
    per_row_anchors = {}
    
    # Process each residual row
    for r in residual_rows:
        elig = alive & d0['thin_decision'].eq('Keep') & d0[row_col].eq(r)
        idx = d0.index[elig]
        n = len(idx)
        if n == 0:
            per_row_quota[r] = 0
            per_row_eligible_idx[r] = idx
            per_row_anchors[r] = pd.Index([])
            continue
        
        quota = int(np.floor(n * removal_fraction))
        quota = max(0, min(quota, n))
        per_row_quota[r] = quota
        
        sub = d0.loc[idx, [dbh_col, 'tree_idx_in_row']]
        k_anchor = max(1, int(np.ceil(n * anchor_fraction)))
        k_anchor = min(k_anchor, n)
        anchors_idx = sub.nlargest(k_anchor, dbh_col).index
        per_row_anchors[r] = anchors_idx
        per_row_eligible_idx[r] = idx
    
    # Calculate influence scores
    scores = {}
    for r in residual_rows:
        anchors_idx = per_row_anchors[r]
        if len(anchors_idx) == 0:
            continue
        
        candidates_idx = [i for i in per_row_eligible_idx[r] if i not in set(anchors_idx)]
        if len(candidates_idx) == 0:
            continue
        
        for cand_id in candidates_idx:
            cand_x = float(d0.at[cand_id, 'tree_idx_in_row'])
            max_score = 0.0
            for anc_id in anchors_idx:
                anc_x = float(d0.at[anc_id, 'tree_idx_in_row'])
                anc_dbh = float(d0.at[anc_id, dbh_col])
                dist = abs(cand_x - anc_x)
                score = anc_dbh / (dist + 1.0)
                if score > max_score:
                    max_score = score
            scores[cand_id] = max_score
    
    # Remove top-scoring trees per row
    selected_idx = []
    per_row_removed = {}
    for r in residual_rows:
        quota = per_row_quota[r]
        if quota <= 0:
            per_row_removed[r] = 0
            continue
        
        anchors_idx = set(per_row_anchors[r])
        row_idxs = [i for i in per_row_eligible_idx[r] if (i not in anchors_idx) and (i in scores)]
        if len(row_idxs) == 0:
            per_row_removed[r] = 0
            continue
        
        row_scores = pd.Series({i: scores[i] for i in row_idxs}).sort_values(ascending=False)
        take = row_scores.index[:quota]
        selected_idx.extend(take.tolist())
        per_row_removed[r] = int(len(take))
    
    if selected_idx:
        d0.loc[selected_idx, 'thin_decision'] = 'Thin'
    
    info = {
        'residual_rows': residual_rows,
        'per_row_quota': {int(k): int(v) for k, v in per_row_quota.items()},
        'anchors_per_row': {int(r): len(per_row_anchors[r]) for r in residual_rows},
        'per_row_removed': {int(k): int(v) for k, v in per_row_removed.items()},
        'n_removed_total': int(len(selected_idx))
    }
    
    return d0, info


def apply_secondary_thin_from_above_anchor(df_after_primary, removal_fraction,
                                            anchor_fraction, neighbors_k, columns):
    """
    Thin from Above-2 (Anchor/Immediate5) - 2D spatial distance
    PRESERVED EXACTLY from original (simplified version without external library)
    """
    from scipy.spatial import cKDTree
    
    d = df_after_primary.copy()
    alive = d['status'].eq('Alive')
    dbh_col = columns['dbh']
    xcol = columns['x_coord']
    ycol = columns['y_coord']
    
    # Get all kept trees (baseline)
    kept_trees = d.loc[alive & d['thin_decision'].eq('Keep')].copy()
    n_kept = len(kept_trees)
    
    if n_kept == 0:
        return d, {'anchors_count': 0, 'removed_total': 0}
    
    # Select anchors (top X% by DBH)
    n_anchors = max(1, int(np.ceil(n_kept * anchor_fraction)))
    anchors = kept_trees.nlargest(n_anchors, dbh_col)
    anchor_indices = set(anchors.index)
    
    # Build KDTree for spatial search
    coords = kept_trees[[xcol, ycol]].values
    tree = cKDTree(coords)
    
    # For each anchor, find k nearest neighbors
    neighbors_to_remove = set()
    for anchor_idx in anchors.index:
        anchor_pos = kept_trees.loc[anchor_idx, [xcol, ycol]].values
        
        # Find k+1 nearest (includes anchor itself)
        distances, indices = tree.query(anchor_pos, k=neighbors_k+1)
        
        # Get actual indices (skip anchor itself)
        neighbor_idx_in_array = [i for i in indices if kept_trees.index[i] != anchor_idx][:neighbors_k]
        neighbor_indices = [kept_trees.index[i] for i in neighbor_idx_in_array]
        
        # Find largest non-anchor neighbors
        for neighbor_idx in neighbor_indices:
            if neighbor_idx not in anchor_indices:
                neighbors_to_remove.add(neighbor_idx)
    
    # Apply removal budget
    n_remove = int(np.floor(n_kept * removal_fraction))
    if len(neighbors_to_remove) > n_remove:
        # Sort by DBH and take largest
        candidates = d.loc[list(neighbors_to_remove), dbh_col].sort_values(ascending=False)
        neighbors_to_remove = set(candidates.head(n_remove).index)
    
    # Mark for removal
    if neighbors_to_remove:
        d.loc[list(neighbors_to_remove), 'thin_decision'] = 'Thin'
    
    info = {
        'anchors_count': len(anchors),
        'removed_total': len(neighbors_to_remove)
    }
    
    return d, info


def apply_secondary_thin_ci_z(df_after_primary, removal_fraction, columns):
    """
    Thin by CI_Z - Remove trees with highest height competition
    PRESERVED EXACTLY from original
    """
    d = df_after_primary.copy()
    alive = d['status'].eq('Alive')
    
    # Get all kept trees
    kept_mask = alive & d['thin_decision'].eq('Keep')
    kept_trees = d[kept_mask].copy()
    
    n_kept = len(kept_trees)
    if n_kept == 0:
        return d, {
            'n_removed_ci_z': 0,
            'ci_z_threshold': np.nan,
            'ci_z_range': (np.nan, np.nan)
        }
    
    # Check if CI_Z column exists
    if 'CI_Z' not in kept_trees.columns:
        raise ValueError("CI_Z column not found. Cannot apply CI_Z-based thinning.")
    
    # Calculate removal quota
    n_remove = int(np.floor(n_kept * removal_fraction))
    if n_remove <= 0:
        return d, {
            'n_removed_ci_z': 0,
            'ci_z_threshold': np.nan,
            'ci_z_range': (kept_trees['CI_Z'].min(), kept_trees['CI_Z'].max())
        }
    
    # Select trees with HIGHEST CI_Z values
    trees_to_remove = kept_trees.nlargest(n_remove, 'CI_Z')
    ci_z_threshold = float(trees_to_remove['CI_Z'].min())
    
    # Mark for removal
    d.loc[trees_to_remove.index, 'thin_decision'] = 'Thin'
    
    info = {
        'n_removed_ci_z': int(len(trees_to_remove)),
        'ci_z_threshold': round(ci_z_threshold, 3),
        'ci_z_range': (round(float(kept_trees['CI_Z'].min()), 3), 
                       round(float(kept_trees['CI_Z'].max()), 3)),
        'avg_ci_z_removed': round(float(trees_to_remove['CI_Z'].mean()), 3),
        'avg_ci_z_kept': round(float(d.loc[d['thin_decision'] == 'Keep', 'CI_Z'].mean()), 3) if len(d[d['thin_decision'] == 'Keep']) > 0 else np.nan
    }
    
    return d, info


def calculate_ci1_for_stand(df, columns, prf=2.708, baf=10):
    """
    Calculate CI1 (Competition Index 1) - Distance-dependent competition
    PRESERVED EXACTLY from original
    """
    from scipy.spatial import cKDTree
    
    d = df.copy()
    dbh_col = columns['dbh']
    xcol = columns['x_coord']
    ycol = columns['y_coord']
    
    # Calculate limiting distance for each tree
    d['LD'] = prf * d[dbh_col]
    d['CI1'] = np.nan
    
    # Get alive trees
    alive_mask = d['status'].eq('Alive') & d[dbh_col].notna()
    alive_trees = d[alive_mask].copy()
    
    if len(alive_trees) == 0:
        return d
    
    # Build KDTree
    coords = alive_trees[[xcol, ycol]].values
    tree = cKDTree(coords)
    
    print(f"Calculating CI1 for {len(alive_trees)} trees...")
    
    # Calculate CI1 for each tree
    for idx, row in alive_trees.iterrows():
        focal_x = row[xcol]
        focal_y = row[ycol]
        focal_dbh = row[dbh_col]
        focal_ld = row['LD']
        
        # Find neighbors within limiting distance
        neighbor_indices = tree.query_ball_point([focal_x, focal_y], focal_ld)
        neighbor_indices = [i for i in neighbor_indices if alive_trees.index[i] != idx]
        
        if len(neighbor_indices) == 0:
            d.at[idx, 'CI1'] = 0.0
            continue
        
        # Get neighbor data
        neighbors = alive_trees.iloc[neighbor_indices].copy()
        neighbors['distance'] = np.sqrt(
            (neighbors[xcol] - focal_x)**2 + 
            (neighbors[ycol] - focal_y)**2
        )
        neighbors = neighbors[neighbors['distance'] > 0]
        
        if len(neighbors) == 0:
            d.at[idx, 'CI1'] = 0.0
            continue
        
        # Calculate CI1 = Σ (neighbor_DBH / focal_DBH) / distance
        ci1 = ((neighbors[dbh_col] / focal_dbh) / neighbors['distance']).sum()
        d.at[idx, 'CI1'] = round(ci1, 3)
    
    return d


def apply_secondary_thin_ci1(df_after_primary, removal_fraction, columns, prf=2.708, baf=10):
    """
    Thin by CI1 - Remove trees with highest distance-dependent competition
    PRESERVED EXACTLY from original
    """
    d = df_after_primary.copy()
    
    # Calculate CI1 if not present
    if 'CI1' not in d.columns:
        print("Calculating CI1...")
        d = calculate_ci1_for_stand(d, columns, prf=prf, baf=baf)
    
    alive = d['status'].eq('Alive')
    kept_mask = alive & d['thin_decision'].eq('Keep')
    kept_trees = d[kept_mask].copy()
    
    n_kept = len(kept_trees)
    if n_kept == 0:
        return d, {
            'n_removed_ci1': 0,
            'ci1_threshold': np.nan,
            'ci1_range': (np.nan, np.nan)
        }
    
    # Check valid CI1 values
    kept_trees_with_ci1 = kept_trees[kept_trees['CI1'].notna()]
    if len(kept_trees_with_ci1) == 0:
        raise ValueError("No valid CI1 values found.")
    
    # Calculate removal quota
    n_remove = int(np.floor(n_kept * removal_fraction))
    if n_remove <= 0:
        return d, {
            'n_removed_ci1': 0,
            'ci1_threshold': np.nan,
            'ci1_range': (kept_trees['CI1'].min(), kept_trees['CI1'].max())
        }
    
    # Select trees with HIGHEST CI1 values
    trees_to_remove = kept_trees_with_ci1.nlargest(n_remove, 'CI1')
    #=======
    # changed nlargest to smallest to get the smallest trees.
    # ======
    ci1_threshold = float(trees_to_remove['CI1'].min())
    
    # Mark for removal
    d.loc[trees_to_remove.index, 'thin_decision'] = 'Thin'
    
    info = {
        'n_removed_ci1': int(len(trees_to_remove)),
        'ci1_threshold': round(ci1_threshold, 3),
        'ci1_range': (round(float(kept_trees['CI1'].min()), 3), 
                      round(float(kept_trees['CI1'].max()), 3)),
        'avg_ci1_removed': round(float(trees_to_remove['CI1'].mean()), 3),
        'avg_ci1_kept': round(float(d.loc[d['thin_decision'] == 'Keep', 'CI1'].mean()), 3) if len(d[d['thin_decision'] == 'Keep']) > 0 else np.nan
    }
    
    return d, info


# ============================================================================
# THINNING STATISTICS (PRESERVED EXACTLY)
# ============================================================================

def calculate_thinning_statistics(df_thinned, columns):
    """
    Calculate comprehensive thinning statistics
    PRESERVED EXACTLY from original
    """
    alive = df_thinned[df_thinned['status'] == 'Alive'].copy()
    kept = alive[alive['thin_decision'] == 'Keep']
    removed = alive[alive['thin_decision'] == 'Thin']
    
    dbh_col = columns['dbh']
    ht_col = columns['height']
    vol_col = columns['volume']
    
    # Calculate basal area (ft²)
    alive['ba_sqft'] = np.pi * (alive[dbh_col] / 2) ** 2 / 144.0
    ba_before = alive['ba_sqft'].sum()
    ba_after = alive.loc[alive['thin_decision'] == 'Keep', 'ba_sqft'].sum()
    ba_removed = ba_before - ba_after
    
    thinning_intensity = ba_removed / ba_before if ba_before > 0 else 0
    
    # Volume statistics
    pre_total_vol = float(alive[vol_col].sum()) if len(alive) else 0.0
    post_total_vol = float(kept[vol_col].sum()) if len(kept) else 0.0
    removed_total_vol = float(removed[vol_col].sum()) if len(removed) else 0.0
    
    # DBH statistics
    pre_mean_dbh = alive[dbh_col].mean()
    post_mean_dbh = kept[dbh_col].mean() if len(kept) > 0 else np.nan
    pre_median_dbh = alive[dbh_col].median()
    post_median_dbh = kept[dbh_col].median() if len(kept) > 0 else np.nan
    
    # Height statistics
    pre_mean_ht = alive[ht_col].mean()
    post_mean_ht = kept[ht_col].mean() if len(kept) > 0 else np.nan
    
    # QMD (Quadratic Mean Diameter)
    pre_qmd = float(np.sqrt(ba_before / len(alive))) if len(alive) > 0 else np.nan
    post_qmd = float(np.sqrt(ba_after / len(kept))) if len(kept) > 0 else np.nan
    
    stats = {
        'trees_before': len(alive),
        'trees_after': len(kept),
        'trees_removed': len(removed),
        'pct_trees_removed': (len(removed) / len(alive) * 100) if len(alive) > 0 else 0,
        'ba_before_sqft': ba_before,
        'ba_after_sqft': ba_after,
        'ba_removed_sqft': ba_removed,
        'ba_removal_pct': thinning_intensity * 100,
        'thinning_intensity': thinning_intensity,  # THE CRITICAL VALUE FOR PTAEDA
        'mean_dbh_before': pre_mean_dbh,
        'mean_dbh_after': post_mean_dbh,
        'median_dbh_before': pre_median_dbh,
        'median_dbh_after': post_median_dbh,
        'mean_height_before': pre_mean_ht,
        'mean_height_after': post_mean_ht,
        'qmd_before': pre_qmd,
        'qmd_after': post_qmd,
        'volume_before': pre_total_vol,
        'volume_after': post_total_vol,
        'volume_removed': removed_total_vol,
    }
    
    return stats


# ============================================================================
# VISUALIZATION (PRESERVED EXACTLY)
# ============================================================================

def plot_thinning_map(df_thinned, output_path, title, columns):
    """Create spatial visualization - PRESERVED EXACTLY"""
    alive = df_thinned[df_thinned['status'] == 'Alive'].copy()
    keep = alive[alive['thin_decision'] == 'Keep']
    thin = alive[alive['thin_decision'] == 'Thin']
    
    xcol = columns['x_coord']
    ycol = columns['y_coord']
    
    fig, ax = plt.subplots(figsize=(10, 8))
    if len(keep) > 0:
        ax.scatter(keep[xcol], keep[ycol], s=8, label='Keep', alpha=0.7, color='steelblue')
    if len(thin) > 0:
        ax.scatter(thin[xcol], thin[ycol], s=12, label='Thin', alpha=0.9, marker='x', color='darkorange')
    
    ax.set_aspect('equal', adjustable='box')
    ax.set_xlabel('X (feet)', fontsize=10)
    ax.set_ylabel('Y (feet)', fontsize=10)
    ax.set_title(title, fontsize=12, fontweight='bold')
    ax.legend(loc='best', fontsize=9)
    ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()


def plot_secondary_thinning_map(df_before_secondary, df_after_secondary, 
                                 output_path, title, columns):
    """
    Create spatial visualization for secondary thinning
    PRESERVED EXACTLY from original
    """
    before_alive = df_before_secondary[df_before_secondary['status'] == 'Alive'].copy()
    after_alive = df_after_secondary[df_after_secondary['status'] == 'Alive'].copy()
    
    xcol = columns['x_coord']
    ycol = columns['y_coord']
    
    # Trees kept after secondary
    keep = after_alive[after_alive['thin_decision'] == 'Keep']
    
    # Trees removed in PRIMARY
    primary_thin = before_alive[before_alive['thin_decision'] == 'Thin']
    
    # Trees removed in SECONDARY
    before_keep_ids = set(before_alive[before_alive['thin_decision'] == 'Keep'].index)
    after_thin_ids = set(after_alive[after_alive['thin_decision'] == 'Thin'].index)
    secondary_thin_ids = before_keep_ids & after_thin_ids
    secondary_thin = after_alive.loc[list(secondary_thin_ids)]
    
    fig, ax = plt.subplots(figsize=(10, 8))
    
    # Plot primary thinning (grey background)
    if len(primary_thin) > 0:
        ax.scatter(primary_thin[xcol], primary_thin[ycol], 
                  s=10, label='Primary Thin', alpha=0.3, 
                  marker='o', color='lightgrey', edgecolors='grey', linewidths=0.3)
    
    # Plot secondary thinning (orange X)
    if len(secondary_thin) > 0:
        ax.scatter(secondary_thin[xcol], secondary_thin[ycol], 
                  s=16, label='Secondary Thin', alpha=0.95, 
                  marker='x', color='darkorange', linewidths=1.5)
    
    # Plot kept trees (blue)
    if len(keep) > 0:
        ax.scatter(keep[xcol], keep[ycol], 
                  s=8, label='Keep', alpha=0.7, color='steelblue')
    
    ax.set_aspect('equal', adjustable='box')
    ax.set_xlabel('X (feet)', fontsize=10)
    ax.set_ylabel('Y (feet)', fontsize=10)
    ax.set_title(title, fontsize=12, fontweight='bold')
    ax.legend(loc='best', fontsize=9, framealpha=0.9)
    ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()


# ============================================================================
# PTAEDA4 GROWTH MODEL - COMPLETE (PRESERVED EXACTLY)
# ============================================================================

def calculate_distance(x1, y1, x2, y2):
    """Calculate Euclidean distance - PRESERVED EXACTLY"""
    return np.sqrt((x2 - x1)**2 + (y2 - y1)**2)


def slope_corrected_prf_fun(dbh, slope, prf):
    """Calculate slope-corrected limiting distance - PRESERVED EXACTLY"""
    if pd.isna(dbh):
        return np.nan
    if pd.isna(slope):
        slope = 0.0
    
    SCF = round(np.sqrt(1 + (slope/100)**2), 3)
    cor_prf = SCF * prf
    LD = cor_prf * dbh
    return round(LD, 3)


def run_ptaeda4_growth_model(input_csv, output_csv, stand_age, age_at_thinning,
                             thinning_intensity, future_age, baf, verbose=True):
    """
    Run PTAEDA4 growth model - PRESERVED EXACTLY from original
    
    All equations, logic, and calculations preserved exactly.
    """
    
    if verbose:
        print("\n" + "="*80)
        print("PTAEDA4 GROWTH MODEL")
        print("="*80)
    
    # Load data
    df_raw = pd.read_csv(input_csv)
    
    if verbose:
        print(f"\nLoaded {len(df_raw)} trees from: {os.path.basename(input_csv)}")
    
    # Prepare input - EXACT format from original
    trees_df = pd.DataFrame({
        'X': df_raw['geom_x'],
        'Y': df_raw['geom_y'],
        'plot': df_raw['plotID'],
        'tree_no': df_raw['treeID'],
        'DBH': df_raw['pDBH_RF'],
        'HT': df_raw['Z_ft'],
        'YST': stand_age,
        'slope': 0.0
    })
    
    if verbose:
        print(f"\nModel Configuration:")
        print(f"  Stand age: {stand_age} years")
        print(f"  Thinning age: {age_at_thinning} years")
        print(f"  Thinning intensity: {thinning_intensity:.1%} BA removal")
        print(f"  Projection: Age {stand_age} → {future_age}")
        print(f"  BAF: {baf}")
    
    # PRF lookup table - PRESERVED EXACTLY
    set_prf_df = pd.DataFrame({
        'BAF': [10, 15, 20, 25, 30, 35, 40, 50, 60],
        'PRF': [2.708, 2.203, 1.902, 1.697, 1.546, 1.428, 1.333, 1.188, 1.081]
    })
    use_prf_no = float(set_prf_df[set_prf_df['BAF'] == baf]['PRF'].values[0])
    
    # Iteration setup - PRESERVED EXACTLY
    no_iterations = future_age - stand_age
    age_seq = list(range(stand_age, future_age + 1))
    
    plot_tree_df_mk2 = trees_df.copy()
    plot_tree_df_mk3 = trees_df[['plot', 'tree_no', 'YST', 'DBH', 'HT']].copy()
    
    if verbose:
        print(f"\nStarting {no_iterations} annual growth iterations...\n")
    
    # =============================================================================
    # MAIN GROWTH ITERATION LOOP - PRESERVED EXACTLY
    # =============================================================================
    
    for k in range(no_iterations):
        current_age = int(age_seq[k])
        
        if verbose:
            print(f"Iteration {k+1}/{no_iterations}: Age {current_age} → {current_age+1}")
        
        # Calculate limiting distances - PRESERVED EXACTLY
        plot_tree_df_mk2['LtD'] = plot_tree_df_mk2.apply(
            lambda row: slope_corrected_prf_fun(row['DBH'], 0, use_prf_no), axis=1
        )
        
        plot_tree_df_mk2['SCLtD'] = plot_tree_df_mk2.apply(
            lambda row: slope_corrected_prf_fun(row['DBH'], row['slope'], use_prf_no), axis=1
        )
        
        # Initialize competition columns - PRESERVED EXACTLY
        plot_tree_df_mk2['CI1'] = np.nan
        plot_tree_df_mk2['CI2'] = np.nan
        plot_tree_df_mk2['BA2'] = np.nan
        
        total_i = len(plot_tree_df_mk2)
        
        # =============================================================================
        # CALCULATE COMPETITION INDICES - PRESERVED EXACTLY
        # =============================================================================
        
        for i in tqdm(range(total_i), desc=f"  Computing competition", leave=False, disable=not verbose):
            
            if pd.isna(plot_tree_df_mk2.iloc[i]['DBH']):
                continue
            
            focal_x = plot_tree_df_mk2.iloc[i]['X']
            focal_y = plot_tree_df_mk2.iloc[i]['Y']
            focal_dbh = plot_tree_df_mk2.iloc[i]['DBH']
            
            if pd.isna(focal_dbh):
                continue
            
            # Calculate distances - PRESERVED EXACTLY
            distances = calculate_distance(
                focal_x, focal_y,
                plot_tree_df_mk2['X'].values,
                plot_tree_df_mk2['Y'].values
            )
            
            # Create neighbor dataframe - PRESERVED EXACTLY
            neighbors = plot_tree_df_mk2.copy()
            neighbors['distance'] = distances
            neighbors = neighbors[
                (neighbors.index != i) &
                (~neighbors['DBH'].isna()) &
                (neighbors['distance'] > 0)
            ]
            
            if len(neighbors) == 0:
                continue
            
            # Filter by limiting distances - PRESERVED EXACTLY
            neighbors_ltd = neighbors[neighbors['distance'] < neighbors['LtD']]
            neighbors_scltd = neighbors[neighbors['distance'] < neighbors['SCLtD']]
            
            # Calculate CI1 - PRESERVED EXACTLY
            if len(neighbors_ltd) > 0:
                ci1 = ((neighbors_ltd['DBH'] / focal_dbh) / neighbors_ltd['distance']).sum()
                plot_tree_df_mk2.at[i, 'CI1'] = round(ci1, 3)
            
            # Calculate CI2 - PRESERVED EXACTLY
            if len(neighbors_scltd) > 0:
                ci2 = ((neighbors_scltd['DBH'] / focal_dbh) / neighbors_scltd['distance']).sum()
                plot_tree_df_mk2.at[i, 'CI2'] = round(ci2, 3)
                
                # Calculate basal area - PRESERVED EXACTLY
                neighbor_ba = (np.pi * (neighbors_scltd['DBH'] / 2)**2).sum()
                focal_ba = np.pi * (focal_dbh / 2)**2
                calc_BA = (neighbor_ba + focal_ba) * baf
                calc_BA = round(calc_BA * 0.00694444, 3)
                plot_tree_df_mk2.at[i, 'BA2'] = calc_BA
        
        # =============================================================================
        # GROWTH CALCULATIONS - PRESERVED EXACTLY
        # =============================================================================
        
        # Dominant height - PRESERVED EXACTLY
        HD = float(plot_tree_df_mk2['HT'].quantile(0.85))
        
        # Site index - PRESERVED EXACTLY
        Xo = 0.5 * (HD - 85.75 + np.sqrt((HD - 85.75)**2 + 4 * 4474 * HD * (current_age**-1.107)))
        SI_25 = (85.75 + Xo) / (1 + (4474 / Xo) * (25**-1.107))
        
        # Dominant height increment - PRESERVED EXACTLY
        HD2 = (147.2 / (1 - (1 - 147.2 / HD) * (current_age / (current_age + 1))**1.17407)) - HD
        
        # =============================================================================
        # THINNING RESPONSE VARIABLES - PRESERVED EXACTLY
        # =============================================================================
        
        years_since_thinning = current_age - age_at_thinning
        
        if years_since_thinning >= 0 and years_since_thinning <= 5:
            BA_before_estimate = plot_tree_df_mk2['BA2'] / (1 - thinning_intensity)
            
            # TRV2 (crown ratio boost) - PRESERVED EXACTLY
            TRV2 = ((plot_tree_df_mk2['BA2'] / BA_before_estimate) *
                    (0.03206 * plot_tree_df_mk2['DBH']**0.43665) *
                    np.exp(-years_since_thinning / (current_age**0.5)))
        else:
            TRV2 = 0
        
        # =============================================================================
        # LIVE CROWN RATIO - PRESERVED EXACTLY
        # =============================================================================
        
        plot_tree_df_mk2['LCR'] = (1.0 -
                                   np.exp((-1.78246 - (34.1967 / current_age)) *
                                         (plot_tree_df_mk2['DBH'] / plot_tree_df_mk2['HT'])) +
                                   TRV2)
        
        # =============================================================================
        # HEIGHT INCREMENT - PRESERVED EXACTLY
        # =============================================================================
        
        plot_tree_df_mk2['HIN'] = (HD2 *
                                   (0.26325 +
                                    2.11119 * plot_tree_df_mk2['LCR']**0.56188 *
                                    np.exp(-0.26375 * plot_tree_df_mk2['CI2'] -
                                          1.03076 * plot_tree_df_mk2['LCR'])))
        
        # =============================================================================
        # DIAMETER INCREMENT - PRESERVED EXACTLY
        # =============================================================================
        
        if years_since_thinning >= 0 and years_since_thinning <= 5:
            BA_before_estimate = plot_tree_df_mk2['BA2'] / (1 - thinning_intensity)
            
            # TRV1 (diameter growth multiplier) - PRESERVED EXACTLY
            plot_tree_df_mk2['TRV1'] = (
                (plot_tree_df_mk2['BA2'] / BA_before_estimate)**((years_since_thinning) / HD**2) *
                np.exp((years_since_thinning)**2 / (current_age / age_at_thinning)**30.829)
            )
        else:
            plot_tree_df_mk2['TRV1'] = 1.0
        
        # DIN calculation - PRESERVED EXACTLY
        plot_tree_df_mk2['DIN'] = (plot_tree_df_mk2['TRV1'] *
                                   (0.286583 * plot_tree_df_mk2['HIN'] + 0.209472) *
                                   (0.8000 * plot_tree_df_mk2['LCR']**0.74295 *
                                    np.exp(-0.69851 * plot_tree_df_mk2['CI2'])))
        
        # =============================================================================
        # MORTALITY - PRESERVED EXACTLY
        # =============================================================================
        
        plot_tree_df_mk2['PLIVE'] = (1.02797 * plot_tree_df_mk2['LCR']**0.03790 *
                                     np.exp(-0.00230 * plot_tree_df_mk2['CI2']**2.65206))
        
        # Apply mortality - PRESERVED EXACTLY
        dead_mask = (plot_tree_df_mk2['PLIVE'] < 0.25) & (~plot_tree_df_mk2['PLIVE'].isna())
        if dead_mask.sum() > 0:
            plot_tree_df_mk2.loc[dead_mask, 'DBH'] = np.nan
            plot_tree_df_mk2.loc[dead_mask, 'HT'] = np.nan
        
        # =============================================================================
        # UPDATE DIMENSIONS - PRESERVED EXACTLY
        # =============================================================================
        
        plot_tree_df_mk2['DIN'] = plot_tree_df_mk2['DIN'].fillna(0)
        plot_tree_df_mk2['HIN'] = plot_tree_df_mk2['HIN'].fillna(0)
        
        # Store projections - PRESERVED EXACTLY
        plot_tree_df_mk3[f'DBH+{k+1}'] = round(plot_tree_df_mk2['DBH'] + plot_tree_df_mk2['DIN'], 3)
        plot_tree_df_mk3[f'HT+{k+1}'] = round(plot_tree_df_mk2['HT'] + plot_tree_df_mk2['HIN'], 3)
        
        # Update for next iteration - PRESERVED EXACTLY
        plot_tree_df_mk2['DBH'] = round(plot_tree_df_mk2['DBH'] + plot_tree_df_mk2['DIN'], 3)
        plot_tree_df_mk2['HT'] = round(plot_tree_df_mk2['HT'] + plot_tree_df_mk2['HIN'], 3)
        
        # Cleanup - PRESERVED EXACTLY
        cols_to_remove = ['CI1', 'CI2', 'BA2', 'LCR', 'HIN', 'TRV1', 'DIN', 'PLIVE', 'LtD', 'SCLtD']
        plot_tree_df_mk2 = plot_tree_df_mk2.drop(columns=cols_to_remove)
    
    # Save output - PRESERVED EXACTLY
    plot_tree_df_mk3.to_csv(output_csv, index=False)
    
    if verbose:
        survival_rate = (plot_tree_df_mk3[f'DBH+{no_iterations}'].notna().sum() / len(plot_tree_df_mk3) * 100)
        print(f"\n✅ Growth projection complete")
        print(f"   Final age: {future_age} years")
        print(f"   Survival rate: {survival_rate:.1f}%")
        print(f"   Output: {os.path.basename(output_csv)}")
    
    return plot_tree_df_mk3


# ============================================================================
# BRIDGE FUNCTIONS - CONNECT THINNING TO PTAEDA4
# ============================================================================

def prepare_ptaeda4_input(df_final, columns, output_csv_path):
    """
    Prepare post-thinning trees for PTAEDA4 input
    
    Args:
        df_final: DataFrame after all thinning (primary + secondary)
        columns: Column configuration dict
        output_csv_path: Where to save PTAEDA4 input CSV
    
    Returns:
        DataFrame ready for PTAEDA4
    """
    # Filter to kept trees only
    kept_trees = df_final[
        (df_final['status'] == 'Alive') &
        (df_final['thin_decision'] == 'Keep')
    ].copy()
    
    # Prepare PTAEDA4 format with proper column names
    ptaeda_input = pd.DataFrame({
        'geom_x': kept_trees[columns['x_coord']].values,
        'geom_y': kept_trees[columns['y_coord']].values,
        'plotID': kept_trees['plotID'].values if 'plotID' in kept_trees.columns else kept_trees.index,
        'treeID': kept_trees['treeID'].values if 'treeID' in kept_trees.columns else kept_trees.index,
        'pDBH_RF': kept_trees[columns['dbh']].values,
        'Z': kept_trees[columns['height']].values / 3.28084,  # Convert feet to meters
        'Z_ft': kept_trees[columns['height']].values
    })
    
    # Save CSV
    ptaeda_input.to_csv(output_csv_path, index=False)
    
    return ptaeda_input


def save_intermediate_results(df_thinned, output_dir, filename_base):
    """Save intermediate thinning results"""
    # Full dataset with all decisions
    full_path = os.path.join(output_dir, f"{filename_base}_full.csv")
    df_thinned.to_csv(full_path, index=False)
    
    # Kept trees only
    kept_trees = df_thinned[
        (df_thinned['status'] == 'Alive') &
        (df_thinned['thin_decision'] == 'Keep')
    ]
    kept_path = os.path.join(output_dir, f"{filename_base}_kept.csv")
    kept_trees.to_csv(kept_path, index=False)
    
    return full_path, kept_path


# ============================================================================
# MAIN WORKFLOW ORCHESTRATOR
# ============================================================================

def run_complete_workflow(config_file):
    """
    Execute complete workflow from configuration
    
    Preserves ALL logic from both thinning tool and PTAEDA4.
    Only adds connection layer.
    """
    
    # Load configuration
    config = load_configuration(config_file)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    verbose = config['output'].get('verbose', True)
    
    if verbose:
        print("\n" + "="*80)
        print("COMPLETE FOREST THINNING AND GROWTH PROJECTION PIPELINE")
        print("="*80)
        print(f"\nRun ID: {timestamp}")
        print(f"Config: {config_file}")
    
    # Create output directory
    output_dir = config['output']['directory']
    os.makedirs(output_dir, exist_ok=True)
    run_dir = os.path.join(output_dir, f"run_{timestamp}")
    os.makedirs(run_dir, exist_ok=True)
    
    # Save copy of config
    config_copy_path = os.path.join(run_dir, "config_used.yaml")
    with open(config_copy_path, 'w') as f:
        yaml.dump(config, f)
    
    columns = config['input']['columns']
    
    # ========================================================================
    # STEP 1: LOAD STAND DATA
    # ========================================================================
    
    if verbose:
        print("\n" + "="*80)
        print("STEP 1: LOADING STAND DATA")
        print("="*80)
    
    df_stand = load_stand_data(config['input']['stand_file'], columns)
    alive_count = len(df_stand[df_stand['status'] == 'Alive'])
    
    if verbose:
        print(f"\n✅ Loaded {alive_count:,} alive trees")
        print(f"   Total rows: {len(ordered_rows(df_stand, columns['row']))}")
    
    # ========================================================================
    # STEP 2: PRIMARY THINNING
    # ========================================================================
    
    if verbose:
        print("\n" + "="*80)
        print("STEP 2: PRIMARY THINNING")
        print("="*80)
    
    primary_config = config['primary_thinning']
    strategy = primary_config['strategy']
    
    if strategy in ["3-row", "4-row", "5-row"]:
        k = int(strategy.split("-")[0])
        start_row = primary_config['start_row']
        if verbose:
            print(f"\nApplying {k}-row thinning (start_row={start_row})...")
        df_primary = k_row_thinning(df_stand, k, start_row, columns)
        
    else:
        # Variable thinning
        variant_map = {
            "variable-3_row_eqv": "3_row_eqv",
            "variable-4_row_eqv": "4_row_eqv",
            "variable-5_row_eqv": "5_row_eqv"
        }
        variant = variant_map[strategy]
        if verbose:
            print(f"\nApplying variable thinning ({variant})...")
        df_primary, cut_rows = apply_variable_thinning(df_stand, variant, columns)
        if verbose:
            print(f"   Cut rows: {cut_rows[:15]}{'...' if len(cut_rows) > 15 else ''}")
    
    primary_stats = calculate_thinning_statistics(df_primary, columns)
    
    if verbose:
        print(f"\n✅ Primary thinning complete:")
        print(f"   Trees removed: {primary_stats['trees_removed']:,} ({primary_stats['pct_trees_removed']:.1f}%)")
        print(f"   BA removal: {primary_stats['ba_removal_pct']:.2f}%")
    
    # Save primary map
    if config['output'].get('create_maps', True):
        primary_map = os.path.join(run_dir, "01_primary_thinning_map.png")
        plot_thinning_map(df_primary, primary_map, f"Primary Thinning: {strategy}", columns)
        if verbose:
            print(f"   Map: {os.path.basename(primary_map)}")
    
    # ========================================================================
    # STEP 3: SECONDARY THINNING (OPTIONAL)
    # ========================================================================
    
    secondary_config = config.get('secondary_thinning', {})
    
    if secondary_config.get('enabled', False):
        if verbose:
            print("\n" + "="*80)
            print("STEP 3: SECONDARY THINNING")
            print("="*80)
        
        sec_strategy = secondary_config['strategy']
        removal_frac = secondary_config.get('removal_fraction', 0.33)
        anchor_frac = secondary_config.get('anchor_fraction', 0.10)
        
        if verbose:
            print(f"\nApplying {sec_strategy}...")
            print(f"   Removal fraction: {removal_frac:.1%}")
            if "Above" in sec_strategy:
                print(f"   Anchor fraction: {anchor_frac:.1%}")
        
        # Apply appropriate secondary strategy
        if sec_strategy == "Thin from Below":
            df_final, sec_info = apply_secondary_thin_from_below(
                df_primary, removal_frac, columns
            )
            n_removed_sec = sec_info['n_removed']
            
        elif sec_strategy == "Thin from Above-1 (Neighbors)":
            df_final, sec_info = apply_secondary_thin_from_above_neighbors(
                df_primary, removal_frac, anchor_frac, columns
            )
            n_removed_sec = sec_info['n_removed_total']
            
        elif sec_strategy == "Thin from Above-2 (Anchor)":
            df_final, sec_info = apply_secondary_thin_from_above_anchor(
                df_primary, removal_frac, anchor_frac, neighbors_k=5, columns=columns
            )
            n_removed_sec = sec_info['removed_total']
            
        elif sec_strategy == "Thin by CI_Z (Height Competition)":
            df_final, sec_info = apply_secondary_thin_ci_z(
                df_primary, removal_frac, columns
            )
            n_removed_sec = sec_info['n_removed_ci_z']
            
        elif sec_strategy == "Thin by CI1 (Distance-Dependent Competition)":
            df_final, sec_info = apply_secondary_thin_ci1(
                df_primary, removal_frac, columns, prf=2.708, baf=10
            )
            n_removed_sec = sec_info['n_removed_ci1']
        else:
            if verbose:
                print(f"   Warning: Unknown strategy '{sec_strategy}', skipping")
            df_final = df_primary
            sec_info = {}
            n_removed_sec = 0
        
        final_stats = calculate_thinning_statistics(df_final, columns)
        
        if verbose:
            print(f"\n✅ Secondary thinning complete:")
            print(f"   Additional removed: {n_removed_sec:,}")
            print(f"   Total BA removal: {final_stats['ba_removal_pct']:.2f}%")
        
        # Save secondary map
        if config['output'].get('create_maps', True):
            secondary_map = os.path.join(run_dir, "02_secondary_thinning_map.png")
            plot_secondary_thinning_map(
                df_primary, df_final, secondary_map,
                f"After Secondary: {sec_strategy}", columns
            )
            if verbose:
                print(f"   Map: {os.path.basename(secondary_map)}")
    else:
        if verbose:
            print("\n⏭️  Skipping secondary thinning")
        df_final = df_primary
        final_stats = primary_stats
    
    # ========================================================================
    # STEP 4: SAVE INTERMEDIATE RESULTS
    # ========================================================================
    
    if config['output'].get('save_intermediate', True):
        if verbose:
            print("\n" + "="*80)
            print("STEP 4: SAVING INTERMEDIATE RESULTS")
            print("="*80)
        
        full_path, kept_path = save_intermediate_results(
            df_final, run_dir, "03_thinning_results"
        )
        
        if verbose:
            print(f"\n✅ Saved intermediate files:")
            print(f"   Full: {os.path.basename(full_path)}")
            print(f"   Kept: {os.path.basename(kept_path)}")
    
    # ========================================================================
    # STEP 5: PREPARE PTAEDA4 INPUT
    # ========================================================================
    
    if verbose:
        print("\n" + "="*80)
        print("STEP 5: PREPARING PTAEDA4 INPUT")
        print("="*80)
    
    ptaeda_input_path = os.path.join(run_dir, "04_ptaeda_input.csv")
    ptaeda_input = prepare_ptaeda4_input(df_final, columns, ptaeda_input_path)
    
    if verbose:
        print(f"\n✅ PTAEDA4 input prepared:")
        print(f"   Trees: {len(ptaeda_input):,}")
        print(f"   Thinning intensity: {final_stats['thinning_intensity']:.3f}")
        print(f"   File: {os.path.basename(ptaeda_input_path)}")
    
    # ========================================================================
    # STEP 6: RUN PTAEDA4 GROWTH MODEL
    # ========================================================================
    
    if verbose:
        print("\n" + "="*80)
        print("STEP 6: RUNNING PTAEDA4 GROWTH MODEL")
        print("="*80)
    
    stand_params = config['stand_parameters']
    growth_params = config.get('growth_model', {})
    
    ptaeda_output_path = os.path.join(run_dir, "05_growth_projections.csv")
    
    growth_df = run_ptaeda4_growth_model(
        input_csv=ptaeda_input_path,
        output_csv=ptaeda_output_path,
        stand_age=stand_params['current_age'],
        age_at_thinning=stand_params['thinning_age'],
        thinning_intensity=final_stats['thinning_intensity'],  # THE CRITICAL BRIDGE
        future_age=stand_params['projection_age'],
        baf=growth_params.get('basal_area_factor', 10),
        verbose=verbose
    )
    
    # ========================================================================
    # STEP 7: GENERATE SUMMARY
    # ========================================================================
    
    if verbose:
        print("\n" + "="*80)
        print("STEP 7: GENERATING SUMMARY")
        print("="*80)
    
    no_years = stand_params['projection_age'] - stand_params['current_age']
    
    # Calculate sums of final predicted values
    final_dbh_col = f'DBH+{no_years}'
    final_ht_col = f'HT+{no_years}'
    sum_final_dbh = float(growth_df[final_dbh_col].sum())
    sum_final_height = float(growth_df[final_ht_col].sum())
    
    summary = {
        'run_id': timestamp,
        'config_file': config_file,
        'configuration': config,
        'thinning_results': {
            'primary_strategy': config['primary_thinning']['strategy'],
            'secondary_strategy': secondary_config.get('strategy', 'None') if secondary_config.get('enabled') else 'None',
            'trees_before': int(final_stats['trees_before']),
            'trees_after': int(final_stats['trees_after']),
            'trees_removed': int(final_stats['trees_removed']),
            'pct_trees_removed': float(final_stats['pct_trees_removed']),
            'ba_before_sqft': float(final_stats['ba_before_sqft']),
            'ba_after_sqft': float(final_stats['ba_after_sqft']),
            'ba_removed_sqft': float(final_stats['ba_removed_sqft']),
            'ba_removal_pct': float(final_stats['ba_removal_pct']),
            'thinning_intensity': float(final_stats['thinning_intensity']),
            'mean_dbh_before': float(final_stats['mean_dbh_before']),
            'mean_dbh_after': float(final_stats['mean_dbh_after']),
            'qmd_before': float(final_stats['qmd_before']),
            'qmd_after': float(final_stats['qmd_after']),
        },
        'growth_projection': {
            'projection_years': no_years,
            'start_age': stand_params['current_age'],
            'final_age': stand_params['projection_age'],
            'final_survival_rate': float(growth_df[final_dbh_col].notna().sum() / len(growth_df) * 100),
            'mean_dbh_final': float(growth_df[final_dbh_col].mean()),
            'mean_height_final': float(growth_df[final_ht_col].mean()),
            'mean_dbh_growth': float((growth_df[final_dbh_col] - growth_df['DBH']).mean()),
            'mean_height_growth': float((growth_df[final_ht_col] - growth_df['HT']).mean()),
            'sum_final_dbh': sum_final_dbh,
            'sum_final_height': sum_final_height,
        },
        'output_files': {
            'config': 'config_used.yaml',
            'primary_map': '01_primary_thinning_map.png' if config['output'].get('create_maps') else None,
            'secondary_map': '02_secondary_thinning_map.png' if (config['output'].get('create_maps') and secondary_config.get('enabled')) else None,
            'thinning_full': '03_thinning_results_full.csv' if config['output'].get('save_intermediate') else None,
            'thinning_kept': '03_thinning_results_kept.csv' if config['output'].get('save_intermediate') else None,
            'ptaeda_input': '04_ptaeda_input.csv',
            'growth_projections': '05_growth_projections.csv',
            'summary': '00_SUMMARY.json'
        }
    }
    
    summary_path = os.path.join(run_dir, "00_SUMMARY.json")
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    
    if verbose:
        print(f"\n✅ Summary saved: {os.path.basename(summary_path)}")
    
    # ========================================================================
    # STEP 8: EXPORT TO EXCEL FOR COMPARISON ANALYSIS
    # ========================================================================
    
    if verbose:
        print("\n" + "="*80)
        print("STEP 8: EXPORTING METRICS TO EXCEL")
        print("="*80)
    
    # Build strategy name
    strategy_name = build_strategy_name(config)
    
    # Calculate metrics from final year columns
    final_dbh_values = growth_df[final_dbh_col].dropna()
    final_ht_values = growth_df[final_ht_col].dropna()
    
    mean_dbh_final = float(final_dbh_values.mean())
    mean_height_final = float(final_ht_values.mean())
    total_volume_final = float(calculate_total_volume(final_dbh_values, final_ht_values))
    
    # Calculate volume after thinning (initial DBH and HT from growth_df)
    initial_dbh_values = growth_df['DBH'].dropna()
    initial_ht_values = growth_df['HT'].dropna()
    volume_after_thinning = float(calculate_total_volume(initial_dbh_values, initial_ht_values))
    
    if verbose:
        print(f"\n📊 Volume Calculations:")
        print(f"   Volume After Thinning: {volume_after_thinning:.2f} cubic feet")
        print(f"   Volume at Final Age: {total_volume_final:.2f} cubic feet")
        print(f"   Growth in Volume: {total_volume_final - volume_after_thinning:.2f} cubic feet")
    
    # Export to Excel
    excel_path = "/Users/amithreddy/Desktop/G&Y_model_python/Growth_Yield_Thinning/Comparison_analysis.xlsx"
    export_to_excel(
        strategy_name=strategy_name,
        mean_dbh=mean_dbh_final,
        mean_height=mean_height_final,
        total_volume=total_volume_final,
        volume_after_thinning=volume_after_thinning,
        excel_path=excel_path,
        verbose=verbose
    )
    
    # ========================================================================
    # FINAL REPORT
    # ========================================================================
    
    if verbose:
        print("\n" + "="*80)
        print("WORKFLOW COMPLETE!")
        print("="*80)
        print(f"\nResults saved to: {run_dir}/")
        print(f"\nKey Results:")
        print(f"  Primary Strategy: {config['primary_thinning']['strategy']}")
        print(f"  Secondary Strategy: {secondary_config.get('strategy', 'None') if secondary_config.get('enabled') else 'None'}")
        print(f"  BA Removal: {final_stats['ba_removal_pct']:.2f}%")
        print(f"  Trees: {final_stats['trees_after']:,} / {final_stats['trees_before']:,}")
        print(f"  Projection: Age {stand_params['current_age']} → {stand_params['projection_age']} ({no_years} years)")
        print(f"  Final Mean DBH: {summary['growth_projection']['mean_dbh_final']:.2f} inches")
        print(f"  Final Mean Height: {summary['growth_projection']['mean_height_final']:.2f} feet")
        print(f"  Sum of Final DBH: {summary['growth_projection']['sum_final_dbh']:.2f} inches")
        print(f"  Sum of Final Height: {summary['growth_projection']['sum_final_height']:.2f} feet")
        print(f"  Survival Rate: {summary['growth_projection']['final_survival_rate']:.1f}%")
        print("="*80 + "\n")
    
    return summary


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    
    if len(sys.argv) < 2:
        print("\nUsage: python complete_pipeline.py config.yaml")
        print("\nExample:")
        print("  python complete_pipeline.py my_config.yaml")
        sys.exit(1)
    
    config_file = sys.argv[1]
    
    try:
        summary = run_complete_workflow(config_file)
        print("\n✅ SUCCESS: Pipeline completed without errors")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ ERROR: Pipeline failed")
        print(f"   {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)