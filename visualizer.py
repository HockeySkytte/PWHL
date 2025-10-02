import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import json
import os
from typing import Dict, List, Optional
import numpy as np


class PWHLVisualizer:
    """Create visualizations from PWHL schedule and game data."""
    
    def __init__(self):
        plt.style.use('default')
        sns.set_palette("husl")
    
    def load_schedule_data(self, filepath: str) -> pd.DataFrame:
        """Load schedule data from CSV or JSON file."""
        if filepath.endswith('.csv'):
            return pd.read_csv(filepath, parse_dates=['date'])
        elif filepath.endswith('.json'):
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Parse JSON data to DataFrame (assuming it's from our scraper)
            from scraper import PWHLScraper
            scraper = PWHLScraper()
            return scraper.parse_schedule_to_dataframe(data)
        else:
            raise ValueError("File must be CSV or JSON format")
    
    def plot_games_by_month(self, df: pd.DataFrame, title: str = "PWHL Games by Month") -> None:
        """Create a bar chart showing number of games by month."""
        if df.empty or 'date' not in df.columns:
            print("No date data available for plotting")
            return
        
        # Extract month from date
        df['month'] = df['date'].dt.month_name()
        games_by_month = df['month'].value_counts()
        
        plt.figure(figsize=(12, 6))
        games_by_month.plot(kind='bar', color='skyblue', edgecolor='black')
        plt.title(title, fontsize=16, fontweight='bold')
        plt.xlabel('Month', fontsize=12)
        plt.ylabel('Number of Games', fontsize=12)
        plt.xticks(rotation=45)
        plt.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        plt.show()
    
    def plot_team_game_counts(self, df: pd.DataFrame, title: str = "Games per Team") -> None:
        """Create a bar chart showing number of games per team."""
        if df.empty:
            print("No data available for plotting")
            return
        
        # Count home and away games for each team
        home_games = df['home_team_name'].value_counts()
        away_games = df['away_team_name'].value_counts()
        
        # Combine home and away games
        all_teams = set(home_games.index.tolist() + away_games.index.tolist())
        team_totals = {}
        
        for team in all_teams:
            home_count = home_games.get(team, 0)
            away_count = away_games.get(team, 0)
            team_totals[team] = home_count + away_count
        
        team_df = pd.DataFrame(list(team_totals.items()), columns=['Team', 'Games'])
        team_df = team_df.sort_values('Games', ascending=True)
        
        plt.figure(figsize=(12, 8))
        bars = plt.barh(team_df['Team'], team_df['Games'], color='lightcoral', edgecolor='black')
        plt.title(title, fontsize=16, fontweight='bold')
        plt.xlabel('Total Games', fontsize=12)
        plt.ylabel('Team', fontsize=12)
        
        # Add value labels on bars
        for bar in bars:
            width = bar.get_width()
            plt.text(width + 0.1, bar.get_y() + bar.get_height()/2, 
                    f'{int(width)}', ha='left', va='center')
        
        plt.grid(axis='x', alpha=0.3)
        plt.tight_layout()
        plt.show()
    
    def plot_attendance_trends(self, df: pd.DataFrame, title: str = "Attendance Trends") -> None:
        """Plot attendance trends over time."""
        if df.empty or 'attendance' not in df.columns or 'date' not in df.columns:
            print("No attendance or date data available for plotting")
            return
        
        # Filter out games without attendance data
        attendance_df = df[df['attendance'].notna() & (df['attendance'] > 0)].copy()
        
        if attendance_df.empty:
            print("No attendance data available")
            return
        
        attendance_df = attendance_df.sort_values('date')
        
        plt.figure(figsize=(14, 6))
        plt.plot(attendance_df['date'], attendance_df['attendance'], 
                marker='o', linestyle='-', alpha=0.7, markersize=4)
        plt.title(title, fontsize=16, fontweight='bold')
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Attendance', fontsize=12)
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        
        # Add average line
        avg_attendance = attendance_df['attendance'].mean()
        plt.axhline(y=avg_attendance, color='red', linestyle='--', alpha=0.7, 
                   label=f'Average: {avg_attendance:.0f}')
        plt.legend()
        
        plt.tight_layout()
        plt.show()
    
    def plot_score_distribution(self, df: pd.DataFrame, title: str = "Score Distribution") -> None:
        """Create histograms showing score distributions."""
        if df.empty or 'home_score' not in df.columns or 'away_score' not in df.columns:
            print("No score data available for plotting")
            return
        
        # Filter completed games
        completed_df = df[(df['home_score'].notna()) & (df['away_score'].notna())].copy()
        
        if completed_df.empty:
            print("No completed games with scores available")
            return
        
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle(title, fontsize=16, fontweight='bold')
        
        # Home team scores
        axes[0, 0].hist(completed_df['home_score'], bins=range(0, int(completed_df['home_score'].max()) + 2), 
                       alpha=0.7, color='skyblue', edgecolor='black')
        axes[0, 0].set_title('Home Team Goals')
        axes[0, 0].set_xlabel('Goals')
        axes[0, 0].set_ylabel('Frequency')
        axes[0, 0].grid(True, alpha=0.3)
        
        # Away team scores
        axes[0, 1].hist(completed_df['away_score'], bins=range(0, int(completed_df['away_score'].max()) + 2), 
                       alpha=0.7, color='lightcoral', edgecolor='black')
        axes[0, 1].set_title('Away Team Goals')
        axes[0, 1].set_xlabel('Goals')
        axes[0, 1].set_ylabel('Frequency')
        axes[0, 1].grid(True, alpha=0.3)
        
        # Total goals per game
        completed_df['total_goals'] = completed_df['home_score'] + completed_df['away_score']
        axes[1, 0].hist(completed_df['total_goals'], bins=range(0, int(completed_df['total_goals'].max()) + 2), 
                       alpha=0.7, color='lightgreen', edgecolor='black')
        axes[1, 0].set_title('Total Goals per Game')
        axes[1, 0].set_xlabel('Total Goals')
        axes[1, 0].set_ylabel('Frequency')
        axes[1, 0].grid(True, alpha=0.3)
        
        # Goal difference
        completed_df['goal_diff'] = abs(completed_df['home_score'] - completed_df['away_score'])
        axes[1, 1].hist(completed_df['goal_diff'], bins=range(0, int(completed_df['goal_diff'].max()) + 2), 
                       alpha=0.7, color='gold', edgecolor='black')
        axes[1, 1].set_title('Goal Difference (Margin of Victory)')
        axes[1, 1].set_xlabel('Goal Difference')
        axes[1, 1].set_ylabel('Frequency')
        axes[1, 1].grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.show()
    
    def create_season_summary(self, df: pd.DataFrame) -> Dict:
        """Create a summary of the season statistics."""
        if df.empty:
            return {"error": "No data available"}
        
        completed_games = df[(df['home_score'].notna()) & (df['away_score'].notna())]
        
        summary = {
            "total_games_scheduled": len(df),
            "games_played": len(completed_games),
            "games_remaining": len(df) - len(completed_games),
        }
        
        if not completed_games.empty:
            summary.update({
                "avg_goals_per_game": (completed_games['home_score'] + completed_games['away_score']).mean(),
                "highest_scoring_game": (completed_games['home_score'] + completed_games['away_score']).max(),
                "most_common_score_home": completed_games['home_score'].mode().iloc[0] if not completed_games['home_score'].mode().empty else None,
                "most_common_score_away": completed_games['away_score'].mode().iloc[0] if not completed_games['away_score'].mode().empty else None,
            })
        
        if 'attendance' in df.columns:
            attendance_data = df[df['attendance'].notna() & (df['attendance'] > 0)]
            if not attendance_data.empty:
                summary.update({
                    "avg_attendance": attendance_data['attendance'].mean(),
                    "max_attendance": attendance_data['attendance'].max(),
                    "min_attendance": attendance_data['attendance'].min(),
                })
        
        return summary


def main():
    """Demonstrate the visualization capabilities."""
    visualizer = PWHLVisualizer()
    
    # Try to load data
    csv_file = "pwhl_schedule_2024_2025_regular.csv"
    json_file = "pwhl_schedule_2024_2025_regular_season.json"
    
    df = None
    if os.path.exists(csv_file):
        print(f"Loading data from {csv_file}")
        df = visualizer.load_schedule_data(csv_file)
    elif os.path.exists(json_file):
        print(f"Loading data from {json_file}")
        df = visualizer.load_schedule_data(json_file)
    else:
        print("No data files found. Please run scraper.py first.")
        return
    
    if df is not None and not df.empty:
        print(f"Loaded {len(df)} games")
        
        # Create visualizations
        visualizer.plot_games_by_month(df, "PWHL 2024/2025 Regular Season - Games by Month")
        visualizer.plot_team_game_counts(df, "PWHL 2024/2025 Regular Season - Games per Team")
        visualizer.plot_attendance_trends(df, "PWHL 2024/2025 Regular Season - Attendance Trends")
        visualizer.plot_score_distribution(df, "PWHL 2024/2025 Regular Season - Score Analysis")
        
        # Print summary
        summary = visualizer.create_season_summary(df)
        print("\n" + "="*50)
        print("SEASON SUMMARY")
        print("="*50)
        for key, value in summary.items():
            if isinstance(value, float):
                print(f"{key.replace('_', ' ').title()}: {value:.2f}")
            else:
                print(f"{key.replace('_', ' ').title()}: {value}")


if __name__ == "__main__":
    main()