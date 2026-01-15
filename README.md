# Thinning Strategy Optimization and Growth Prediction in Loblolly Pine Plantations

A comprehensive Python pipeline for evaluating and comparing different thinning strategies in loblolly pine plantations using UAV LiDAR data and individual tree growth models.

## Overview

This project integrates thinning analysis with the PTAEDA4 individual tree growth model to predict long-term stand development under various silvicultural treatments. The pipeline supports both systematic row-removal patterns and advanced competition-based thinning strategies, enabling forestry researchers and managers to make data-driven decisions about plantation management.

## Key Features

- **Multiple Thinning Strategies**: Compare systematic (3-row, 4-row, 5-row) and variable row thinning patterns
- **Competition-Based Thinning**: Advanced methods using height competition (CI_Z) and distance-dependent competition (CI1) indices
- **Growth Projection**: Integration with PTAEDA4 growth model for accurate yield predictions
- **Automated Analysis**: Complete workflow from raw stand data to comparative metrics
- **Spatial Visualization**: Generate maps showing thinning patterns and tree distributions
- **Excel Export**: Automated comparison analysis with key metrics for all strategies

## Installation

### Prerequisites

- Python 3.8 or higher
- pip package manager

### Required Packages

```bash
pip install pandas numpy matplotlib pyyaml tqdm openpyxl scipy
```

### Clone Repository

```bash
git clone https://github.com/yourusername/thinning-optimization.git
cd thinning-optimization
```

## Usage

### Basic Workflow

1. **Prepare your configuration file** (`pipeline_config.yaml`)
2. **Run the pipeline**:

```bash
python complete_pipeline.py pipeline_config.yaml
```

### Configuration

The pipeline is controlled via a YAML configuration file. Here's a minimal example:

```yaml
input:
  stand_file: "path/to/your/stand_data.csv"
  columns:
    row: "NL"
    x_coord: "X1"
    y_coord: "Y1"
    dbh: "pDBH_RF"
    height: "Z_ft"

primary_thinning:
  strategy: "4-row"
  start_row: 1

secondary_thinning:
  enabled: true
  strategy: "Thin by CI1 (Distance-Dependent Competition)"
  removal_fraction: 0.20

stand_parameters:
  current_age: 24
  thinning_age: 24
  projection_age: 29

output:
  directory: "results"
  save_intermediate: true
  create_maps: true
```

## Input Data Format

Your stand data CSV should include:
- **Row number** (planting row identifier)
- **X coordinate** (feet)
- **Y coordinate** (feet)
- **DBH** (diameter at breast height, inches)
- **Height** (total tree height, feet)

Example:
```csv
NL,X1,Y1,pDBH_RF,Z_ft
1,10.5,5.2,8.3,45.6
1,20.3,5.1,9.1,47.2
2,10.8,15.4,7.9,44.8
```

## Thinning Strategies

### Primary Thinning

**Systematic Row Removal:**
- `3-row`: Remove every 3rd row (~33% removal)
- `4-row`: Remove every 4th row (~25% removal)
- `5-row`: Remove every 5th row (~20% removal)

**Variable Thinning (Optimized):**
- `variable-3_row_eqv`: Removes ~R/3 rows with optimized spacing
- `variable-4_row_eqv`: Removes ~R/4 rows with optimized spacing
- `variable-5_row_eqv`: Removes ~R/5 rows with optimized spacing

### Secondary Thinning

- **Thin from Below**: Removes smallest trees stand-wide
- **Thin from Above-1**: Removes competitors near large anchor trees (1D distance)
- **Thin from Above-2**: Removes competitors near large anchor trees (2D spatial distance)
- **Thin by CI_Z**: Targets trees with highest height competition
- **Thin by CI1**: Targets trees with highest distance-dependent competition (PTAEDA4 model)

## Output

### File Structure

```
results/
└── YYYYMMDD_HHMMSS/
    ├── 00_SUMMARY.json                      # Complete run summary
    ├── 01_primary_thinning_map.png          # Spatial visualization
    ├── 02_secondary_thinning_map.png        # Secondary thinning map
    ├── 03_thinning_results_full.csv         # All trees with decisions
    ├── 03_thinning_results_kept.csv         # Kept trees only
    ├── 04_ptaeda_input.csv                  # PTAEDA4 model input
    ├── 05_growth_projections.csv            # Final predictions
    └── config_used.yaml                     # Configuration snapshot
```

### Excel Comparison Analysis

The pipeline automatically exports key metrics to `Comparison_analysis.xlsx`:

| Strategy | Mean_DBH | Mean_Height | Total_Volume | Volume_After_Thinning | Growth_in_Volume |
|----------|----------|-------------|--------------|----------------------|------------------|
| 4-row_start1_CI1_0.20 | 10.45 | 52.3 | 1250.5 | 890.2 | 360.3 |

## Methodology

### Volume Calculation

Stand volume is calculated using the Tasissa et al. formula:

```
V_ob = 0.25663 + 0.00239 × D² × H
```

Where:
- `D` = DBH (inches)
- `H` = Height (feet)
- `V_ob` = Outside bark volume (cubic feet)

### Growth Model

The pipeline uses the PTAEDA4 individual tree growth model with:
- Competition indices (CI1, CI2) for neighbor interactions
- Thinning response variables accounting for accelerated growth
- Site index calculation based on dominant height
- Annual diameter and height increment predictions

## Example Results

```
Key Results:
  Primary Strategy: 4-row
  Secondary Strategy: Thin by CI1 (Distance-Dependent Competition)
  BA Removal: 35.42%
  Trees: 1,234 / 1,890
  Projection: Age 24 → 29 (5 years)
  Final Mean DBH: 10.45 inches
  Final Mean Height: 52.31 feet
  Growth in Volume: 360.30 cubic feet
  Survival Rate: 98.5%
```

## Project Structure

```
.
├── complete_pipeline.py          # Main pipeline orchestrator
├── pipeline_config.yaml          # Configuration file
├── README.md                     # This file
└── results/                      # Output directory
```

## Applications

This pipeline has been used for:
- Evaluating thinning strategies across multiple Kentucky study sites
- Optimizing corridor spacing for harvesting operations
- Comparing competition-based vs. systematic thinning approaches
- Predicting long-term stand development under different management scenarios


## Acknowledgments

- PTAEDA4 growth model based on original R implementation
- Tasissa et al. volume equations for loblolly pine
- UAV LiDAR data collection and processing methodologies

## Contact

For questions:
- **Author**: Amith Reddy Atla
- **Email**: atlaamit@msu.edu
- **Institution**: Michigan State University

