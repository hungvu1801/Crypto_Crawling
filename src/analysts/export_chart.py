import pandas as pd
from datetime import datetime, timedelta

import matplotlib.pyplot as plt
import seaborn as sns
from src.config import DATA_DIR, TRANSACT_PERIOD
from src.utility.data_processing import process_merge, scientific_to_float

import matplotlib.font_manager as fm
plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']
plt.rcParams['font.sans-serif'] = ['SimHei Regular']
plt.rcParams['font.sans-serif'] = ['Malgun Gothic']
plt.rcParams['axes.unicode_minus'] = False
# plt.rcParams['font.sans-serif'] = ['Noto Sans TC Regular']
import warnings
warnings.filterwarnings("ignore")

def export_charts(today) -> None:
    df_list = list()
    for date_delta in range(100):
        date = (datetime.today() - timedelta(days=date_delta)).strftime('%y%m%d')
        try:
            df = pd.read_csv(f"{DATA_DIR}/{date}/data_merge.csv", header=0)
            df['transact_date'] = date
        except Exception:
            continue
        df_list.append(df)

    df_merge = pd.concat(df_list, axis=0, ignore_index=True)
    df_filter_no_data = process_merge(df_merge)
    df_filter_no_data.drop_duplicates(subset=['user_id', 'crypto_exchange', 'transact_date'], inplace=True)
    df_sorted_ROI = df_filter_no_data.loc[df_filter_no_data['transact_date'] == today].sort_values(['crypto_exchange', 'ROI'], ascending=[True, False])
    #####################################################################################################
    df_sorted_ROI.drop_duplicates(inplace=True)
    top_5_traders_byROI = df_sorted_ROI.groupby('crypto_exchange').head(5)

    plt.figure(figsize=(15, 8))
    palette = sns.color_palette("Set2")

    sns.barplot(x='trader_name', y='ROI', data=top_5_traders_byROI)
    # Plotting
    ax = sns.barplot(x='trader_name', y='ROI', hue='crypto_exchange', data=top_5_traders_byROI, palette=palette)

    # Add numbers on top of bars
    for container in ax.containers:
        ax.bar_label(container, fmt='{:,.2f}', label_type='edge', padding=3)
        
    plt.title('Top 5 Traders by ROI for Each Crypto Exchange in 7D transact_period')
    plt.xlabel('Trader Name', fontsize=14, weight='bold')
    plt.ylabel('ROI (%)', fontsize=14, weight='bold')
    plt.xticks(rotation=45, fontsize=8)
    plt.yticks(fontsize=12)
    plt.legend(title='Crypto Exchange', title_fontsize='13', fontsize='11', loc='upper right')
    plt.grid(True, linestyle='--', alpha=0.7) 
    plt.tight_layout()
    plt.savefig(f"reports/{today}_top5.png")
    # plt.show()

    #####################################################################################################
    # Group by crypto_exchange and calculate mean ROI and PNL
    grouped_stats = df_filter_no_data.groupby(['crypto_exchange', 'transact_date']).agg({
        'ROI': ['mean', 'std', 'min', 'max'],
        'PNL': ['mean', 'std', 'min', 'max']
    })
    grouped_stats.columns = ['_'.join(col) for col in grouped_stats.columns.values]
    grouped_stats.reset_index(inplace=True)
    grouped_stats['transact_date'] = pd.to_datetime(grouped_stats['transact_date'], format='%y%m%d')
    grouped_stats['PNL_min'] = grouped_stats['PNL_min'].apply(scientific_to_float)
    grouped_stats['PNL_max'] = grouped_stats['PNL_max'].apply(scientific_to_float)
    grouped_stats.to_csv(f"reports/{today}_groupstats.csv", index=False)
    # Set up the matplotlib figure
    #####################################################################################################
    # Set up the matplotlib figure
    fig, axes = plt.subplots(1, 2, figsize=(15, 6), sharey=False)

    # Plot ROI mean
    sns.lineplot(
        ax=axes[0], data=grouped_stats, 
        x='transact_date', y='ROI_mean', 
        hue='crypto_exchange', marker='o', 
        linewidth=0.5, markersize=4)
    axes[0].set_title('ROI Mean Over Time by Exchange')
    axes[0].set_xlabel('Date')
    axes[0].set_ylabel('ROI Mean')
    axes[0].tick_params(axis='x', rotation=45)
    axes[0].yaxis.grid(True)
    # Plot PNL mean
    sns.lineplot(
        ax=axes[1], data=grouped_stats, 
        x='transact_date', y='PNL_mean', 
        hue='crypto_exchange', marker='o', 
        linewidth=0.5, markersize=4)
    axes[1].set_title('PNL Mean Over Time by Exchange')
    axes[1].set_xlabel('Date')
    axes[1].set_ylabel('PNL Mean')
    axes[1].tick_params(axis='x', rotation=45)
    axes[1].yaxis.grid(True)
    plt.savefig(f"reports/{today}_roiandpnl.png")
    # plt.tight_layout()
    # plt.show()

    #####################################################################################################
    # Scatter plot for ROI vs. PNL
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=grouped_stats, x='ROI_mean', y='PNL_mean', hue='crypto_exchange', style='crypto_exchange', s=100)
    plt.title('ROI vs. PNL by Exchange')
    plt.xlabel('ROI Mean')
    plt.ylabel('PNL Mean')
    plt.grid(True)
    plt.savefig(f"reports/{today}_roivspnlscatter.png")
    # plt.show()