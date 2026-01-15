"""
PTAEDA4 Growth Model - Configured from Row Thinning Analysis
"""

import pandas as pd
import numpy as np
from tqdm import tqdm
import warnings
import os
warnings.filterwarnings('ignore')

# Input file
IO_trees_csv = "/Users/amithreddy/Desktop/G&Y_model_python/row_thinning_results/ptaeda_input_sparse_3row.csv"

# Stand age configuration
STAND_AGE = 24

# Thinning configuration
AGE_AT_THINNING = 24
THINNING_INTENSITY = 0.316  # 31.63% BA removal

# Model parameters (default)
use_baf_no = 10
SAge = 24
future_age = 29  # Adjust as needed

print("="*80)
print("PTAEDA4 GROWTH MODEL - POST-THINNING PROJECTION")
print("="*80)

df_raw = pd.read_csv(IO_trees_csv)
print(f"\n✅ Loaded {len(df_raw)} trees from: {IO_trees_csv}")

# Create model input
trees_df = pd.DataFrame({
    'X': df_raw['geom_x'],
    'Y': df_raw['geom_y'],
    'plot': df_raw['plotID'],
    'tree_no': df_raw['treeID'],
    'DBH': df_raw['pDBH_RF'],
    'HT': df_raw['Z_ft'],
    'YST': STAND_AGE,
    'slope': 0.0
})

print(f"\nStand Configuration:")
print(f"  Current age: {STAND_AGE} years")
print(f"  Thinned at age: {AGE_AT_THINNING} years")
print(f"  Thinning intensity: {THINNING_INTENSITY:.1%} BA removal")
print(f"  Projection: Age {SAge} → Age {future_age}")


# =============================================================================
# DEFINE PLOT RADIUS FACTORS
# =============================================================================

set_prf_df = pd.DataFrame({
    'BAF': [10, 15, 20, 25, 30, 35, 40, 50, 60],
    'PRF': [2.708, 2.203, 1.902, 1.697, 1.546, 1.428, 1.333, 1.188, 1.081]
})

use_prf_no = float(set_prf_df[set_prf_df['BAF'] == use_baf_no]['PRF'].values[0])

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def calculate_distance(x1, y1, x2, y2):
    """Calculate Euclidean distance between two points"""
    return np.sqrt((x2 - x1)**2 + (y2 - y1)**2)

def slope_corrected_prf_fun(dbh, slope, prf):
    """Calculate slope-corrected limiting distance"""
    if pd.isna(dbh):
        return np.nan
    if pd.isna(slope):
        slope = 0.0
    
    SCF = round(np.sqrt(1 + (slope/100)**2), 3)
    cor_prf = SCF * prf
    LD = cor_prf * dbh
    return round(LD, 3)

# =============================================================================
# PREPARE FOR ITERATIONS
# =============================================================================

no_iterations = future_age - SAge
age_seq = list(range(SAge, future_age + 1))

# Working dataframe
plot_tree_df_mk2 = trees_df.copy()

# Output dataframe
plot_tree_df_mk3 = trees_df[['plot', 'tree_no', 'YST', 'DBH', 'HT']].copy()

print(f"\n{'='*80}")
print(f"STARTING GROWTH PROJECTION")
print(f"Age {SAge} → {future_age} ({no_iterations} annual iterations)")
print(f"Post-thinning growth modeling ENABLED")
print(f"{'='*80}\n")

# =============================================================================
# MAIN GROWTH ITERATION LOOP
# =============================================================================

for k in range(no_iterations):
    
    print(f"\n{'='*80}")
    print(f"ITERATION {k+1}/{no_iterations}: Age {age_seq[k]} → Age {age_seq[k+1]}")
    print(f"{'='*80}\n")
    
    current_age = int(age_seq[k])
    
    # Calculate limiting distances
    print("Calculating limiting distances...")
    plot_tree_df_mk2['LtD'] = plot_tree_df_mk2.apply(
        lambda row: slope_corrected_prf_fun(row['DBH'], 0, use_prf_no), axis=1
    )
    
    plot_tree_df_mk2['SCLtD'] = plot_tree_df_mk2.apply(
        lambda row: slope_corrected_prf_fun(row['DBH'], row['slope'], use_prf_no), axis=1
    )
    
    largest_LtD = plot_tree_df_mk2['SCLtD'].max()
    
    # Initialize competition columns
    plot_tree_df_mk2['CI1'] = np.nan
    plot_tree_df_mk2['CI2'] = np.nan
    plot_tree_df_mk2['BA2'] = np.nan
    
    total_i = len(plot_tree_df_mk2)
    
    # =============================================================================
    # CALCULATE COMPETITION INDICES (Distance-Dependent)
    # =============================================================================
    
    print("Calculating competition indices for each tree...")
    
    for i in tqdm(range(total_i), desc="Processing trees"):
        
        # Check mortality of focal tree
        if pd.isna(plot_tree_df_mk2.iloc[i]['DBH']):
            continue
        
        focal_x = plot_tree_df_mk2.iloc[i]['X']
        focal_y = plot_tree_df_mk2.iloc[i]['Y']
        focal_dbh = plot_tree_df_mk2.iloc[i]['DBH']
        
        if pd.isna(focal_dbh):
            continue
        
        # Calculate distances to all other trees
        distances = calculate_distance(
            focal_x, focal_y,
            plot_tree_df_mk2['X'].values,
            plot_tree_df_mk2['Y'].values
        )
        
        # Create neighbor dataframe (exclude self and dead trees)
        neighbors = plot_tree_df_mk2.copy()
        neighbors['distance'] = distances
        neighbors = neighbors[
            (neighbors.index != i) &  # Exclude self
            (~neighbors['DBH'].isna()) &  # Exclude dead trees
            (neighbors['distance'] > 0)  # Exclude duplicates
        ]
        
        if len(neighbors) == 0:
            continue
        
        # Filter by limiting distances
        neighbors_ltd = neighbors[neighbors['distance'] < neighbors['LtD']]
        neighbors_scltd = neighbors[neighbors['distance'] < neighbors['SCLtD']]
        
        # Calculate CI1 (no slope correction)
        if len(neighbors_ltd) > 0:
            ci1 = ((neighbors_ltd['DBH'] / focal_dbh) / neighbors_ltd['distance']).sum()
            plot_tree_df_mk2.at[i, 'CI1'] = round(ci1, 3)
        
        # Calculate CI2 (with slope correction)
        if len(neighbors_scltd) > 0:
            ci2 = ((neighbors_scltd['DBH'] / focal_dbh) / neighbors_scltd['distance']).sum()
            plot_tree_df_mk2.at[i, 'CI2'] = round(ci2, 3)
            
            # Calculate basal area
            neighbor_ba = (np.pi * (neighbors_scltd['DBH'] / 2)**2).sum()
            focal_ba = np.pi * (focal_dbh / 2)**2
            calc_BA = (neighbor_ba + focal_ba) * use_baf_no
            calc_BA = round(calc_BA * 0.00694444, 3)
            plot_tree_df_mk2.at[i, 'BA2'] = calc_BA
    
    # =============================================================================
    # CALCULATE GROWTH RELATIONSHIPS
    # =============================================================================
    
    print("\nCalculating growth equations...")
    
    # Dominant height (85th percentile)
    HD = float(plot_tree_df_mk2['HT'].quantile(0.85))
    
    # Site index using Rachel's equation
    Xo = 0.5 * (HD - 85.75 + np.sqrt((HD - 85.75)**2 + 4 * 4474 * HD * (current_age**-1.107)))
    SI_25 = (85.75 + Xo) / (1 + (4474 / Xo) * (25**-1.107))
    SI_25 = round(SI_25, 3)
    
    # Dominant height increment
    HD2 = (147.2 / (1 - (1 - 147.2 / HD) * (current_age / (current_age + 1))**1.17407)) - HD
    
    # =============================================================================
    # THINNING RESPONSE VARIABLES
    # =============================================================================
    
    years_since_thinning = current_age - AGE_AT_THINNING
    
    if years_since_thinning >= 0 and years_since_thinning <= 5:
        Thin_TF = 1
        BA_before_estimate = plot_tree_df_mk2['BA2'] / (1 - THINNING_INTENSITY)
        
        # TRV2 (crown ratio boost)
        TRV2 = (Thin_TF * 
                (plot_tree_df_mk2['BA2'] / BA_before_estimate) * 
                (0.03206 * plot_tree_df_mk2['DBH']**0.43665) * 
                np.exp(-years_since_thinning / (current_age**0.5)))
        
        print(f"   Thinning response active: {years_since_thinning} years post-thin")
    else:
        Thin_TF = 0
        TRV2 = 0
        print(f"   Thinning response inactive")
    
    # =============================================================================
    # LIVE CROWN RATIO (LCR)
    # =============================================================================
    
    plot_tree_df_mk2['LCR'] = (1.0 - 
                               np.exp((-1.78246 - (34.1967 / current_age)) * 
                                     (plot_tree_df_mk2['DBH'] / plot_tree_df_mk2['HT'])) + 
                               TRV2)
    
    # =============================================================================
    # HEIGHT INCREMENT (HIN)
    # =============================================================================
    
    plot_tree_df_mk2['HIN'] = (HD2 * 
                               (0.26325 + 
                                2.11119 * plot_tree_df_mk2['LCR']**0.56188 * 
                                np.exp(-0.26375 * plot_tree_df_mk2['CI2'] - 
                                      1.03076 * plot_tree_df_mk2['LCR'])))
    
    # =============================================================================
    # DIAMETER INCREMENT (DIN)
    # =============================================================================
    
    if years_since_thinning >= 0 and years_since_thinning <= 5:
        BA_before_estimate = plot_tree_df_mk2['BA2'] / (1 - THINNING_INTENSITY)
        
        plot_tree_df_mk2['TRV1'] = (
            (plot_tree_df_mk2['BA2'] / BA_before_estimate)**((years_since_thinning) / HD**2) * 
            np.exp((years_since_thinning)**2 / (current_age / AGE_AT_THINNING)**30.829)
        )
    else:
        plot_tree_df_mk2['TRV1'] = 1.0
    
    plot_tree_df_mk2['DIN'] = (plot_tree_df_mk2['TRV1'] * 
                               (0.286583 * plot_tree_df_mk2['HIN'] + 0.209472) * 
                               (0.8000 * plot_tree_df_mk2['LCR']**0.74295 * 
                                np.exp(-0.69851 * plot_tree_df_mk2['CI2'])))
    
    # =============================================================================
    # MORTALITY PROBABILITY (PLIVE)
    # =============================================================================
    
    plot_tree_df_mk2['PLIVE'] = (1.02797 * plot_tree_df_mk2['LCR']**0.03790 * 
                                 np.exp(-0.00230 * plot_tree_df_mk2['CI2']**2.65206))
    
    # Apply mortality
    dead_mask = (plot_tree_df_mk2['PLIVE'] < 0.25) & (~plot_tree_df_mk2['PLIVE'].isna())
    if dead_mask.sum() > 0:
        plot_tree_df_mk2.loc[dead_mask, 'DBH'] = np.nan
        plot_tree_df_mk2.loc[dead_mask, 'HT'] = np.nan
        print(f"   Mortality: {dead_mask.sum()} trees")
    
    # =============================================================================
    # UPDATE TREE DIMENSIONS
    # =============================================================================
    
    plot_tree_df_mk2['DIN'] = plot_tree_df_mk2['DIN'].fillna(0)
    plot_tree_df_mk2['HIN'] = plot_tree_df_mk2['HIN'].fillna(0)
    
    # Store projections
    plot_tree_df_mk3[f'DBH+{k+1}'] = round(plot_tree_df_mk2['DBH'] + plot_tree_df_mk2['DIN'], 3)
    plot_tree_df_mk3[f'HT+{k+1}'] = round(plot_tree_df_mk2['HT'] + plot_tree_df_mk2['HIN'], 3)
    
    # Update for next iteration
    plot_tree_df_mk2['DBH'] = round(plot_tree_df_mk2['DBH'] + plot_tree_df_mk2['DIN'], 3)
    plot_tree_df_mk2['HT'] = round(plot_tree_df_mk2['HT'] + plot_tree_df_mk2['HIN'], 3)
    
    # Report summary
    print(f"\n   Site Index (base 25): {SI_25} ft")
    print(f"   Dominant Height: {round(HD, 2)} ft")
    print(f"   Mean Height Increment: {plot_tree_df_mk2['HIN'].mean():.2f} ft")
    print(f"   Mean DBH Increment: {plot_tree_df_mk2['DIN'].mean():.2f} in")
    print(f"   Mean TRV1: {plot_tree_df_mk2['TRV1'].mean():.3f}")
    print(f"   Trees alive: {(~plot_tree_df_mk2['DBH'].isna()).sum()}/{total_i}")
    
    # Clean up
    cols_to_remove = ['CI1', 'CI2', 'BA2', 'LCR', 'HIN', 'TRV1', 'DIN', 'PLIVE', 'LtD', 'SCLtD']
    plot_tree_df_mk2 = plot_tree_df_mk2.drop(columns=cols_to_remove)

# =============================================================================
# WRITE OUTPUT
# =============================================================================

print("\n" + "="*80)
print("GROWTH PROJECTION COMPLETE")
print("="*80)
output_dir = "/Users/amithreddy/Desktop/G&Y_model_python/FINAL_Growth_predictions"
output_file = os.path.join(output_dir, "growth_output.csv")
plot_tree_df_mk3.to_csv(output_file, index=False)

print(f"\n✅ Output written to: {output_file}")
print(f"   Total trees: {len(plot_tree_df_mk3)}")
print(f"   Projected ages: {SAge} → {future_age}")

print(f"\nOutput columns:")
print(f"   {plot_tree_df_mk3.columns.tolist()}")

print("\n" + "="*80)
print("Summary Statistics")
print("="*80)

print(f"\nInitial (Age {SAge}):")
print(f"   DBH: {plot_tree_df_mk3['DBH'].min():.2f} - {plot_tree_df_mk3['DBH'].max():.2f} in")
print(f"   Height: {plot_tree_df_mk3['HT'].min():.2f} - {plot_tree_df_mk3['HT'].max():.2f} ft")

final_dbh_col = f'DBH+{no_iterations}'
final_ht_col = f'HT+{no_iterations}'
print(f"\nFinal (Age {future_age}):")
print(f"   DBH: {plot_tree_df_mk3[final_dbh_col].min():.2f} - {plot_tree_df_mk3[final_dbh_col].max():.2f} in")
print(f"   Height: {plot_tree_df_mk3[final_ht_col].min():.2f} - {plot_tree_df_mk3[final_ht_col].max():.2f} ft")

print(f"\nTotal Growth ({no_iterations} years):")
print(f"   Mean DBH increment: {(plot_tree_df_mk3[final_dbh_col] - plot_tree_df_mk3['DBH']).mean():.2f} in")
print(f"   Mean height increment: {(plot_tree_df_mk3[final_ht_col] - plot_tree_df_mk3['HT']).mean():.2f} ft")

survival_rate = (plot_tree_df_mk3[final_dbh_col].notna().sum() / len(plot_tree_df_mk3)) * 100
print(f"   Survival rate: {survival_rate:.1f}%")

print("\n" + "="*80)
print("Model run complete!")
print("="*80)
