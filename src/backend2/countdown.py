"""
Countdown calculation logic for playoffs and season dates

Provides countdown calculations for season and playoff events using
Backend v2 data, with fallback logic when API data is unavailable.
"""
from datetime import datetime, date, timedelta
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class CountdownCalculator:
    """Calculates countdowns for season and playoff events"""
    
    def __init__(self, season_data: Dict[str, Any] = None, playoff_data: Dict[str, Any] = None):
        self.season_data = season_data or {}
        self.playoff_data = playoff_data or {}
        self.current_date = date.today()
    
    def get_season_countdown(self) -> Dict[str, Any]:
        """Calculate countdown to season start"""
        season_start = self._parse_date(
            self.season_data.get('regularSeasonStartDate') or 
            self.season_data.get('startDate')
        )
        
        if not season_start:
            # Fallback calculation based on typical NHL schedule
            return self._calculate_estimated_season_start()
        
        days_until = (season_start - self.current_date).days
        
        return {
            'days_until_season': max(0, days_until),
            'season_start_date': season_start,
            'season_started': days_until <= 0,
            'season_year': self._get_season_year_string(season_start),
            'estimated': False
        }
    
    def get_playoff_countdown(self) -> Dict[str, Any]:
        """Calculate countdown to playoffs"""
        # Try to extract playoff start from season data
        playoff_start = self._parse_date(
            self.season_data.get('playoffStartDate')
        )
        
        if not playoff_start:
            # Estimate based on season end + typical gap
            season_end = self._parse_date(
                self.season_data.get('regularSeasonEndDate') or
                self.season_data.get('endDate')
            )
            if season_end:
                playoff_start = season_end + timedelta(days=5)  # Typical gap
        
        if not playoff_start:
            return self._calculate_estimated_playoff_start()
        
        days_until = (playoff_start - self.current_date).days
        
        return {
            'days_until_playoffs': max(0, days_until),
            'playoff_start_date': playoff_start,
            'playoffs_started': days_until <= 0,
            'playoff_status': self._get_playoff_status(),
            'estimated': not bool(self.season_data.get('playoffStartDate'))
        }
    
    def is_playoff_season(self) -> bool:
        """Check if we're currently in playoff season"""
        playoff_info = self.get_playoff_countdown()
        return playoff_info.get('playoffs_started', False)
    
    def is_offseason(self) -> bool:
        """Check if we're in the offseason"""
        season_info = self.get_season_countdown()
        playoff_info = self.get_playoff_countdown()
        
        # Off-season is after playoffs end and before season starts
        if season_info.get('season_started', False):
            return False
        
        # If playoffs have started, we're not in offseason
        if playoff_info.get('playoffs_started', False):
            return False
        
        return True
    
    def _parse_date(self, date_str: str) -> Optional[date]:
        """Parse date string from API"""
        if not date_str:
            return None
        
        try:
            # Try multiple date formats
            for fmt in ['%Y-%m-%d', '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S']:
                try:
                    return datetime.strptime(date_str, fmt).date()
                except ValueError:
                    continue
            return None
        except Exception as e:
            logger.warning(f"Failed to parse date {date_str}: {e}")
            return None
    
    def _calculate_estimated_season_start(self) -> Dict[str, Any]:
        """Fallback calculation when API data unavailable"""
        current_year = self.current_date.year
        current_month = self.current_date.month
        
        # NHL typically starts in early October
        if current_month >= 10:
            # If we're in October or later, next season starts next year
            estimated_start = date(current_year + 1, 10, 7)
        else:
            # If we're before October, season might start this year
            estimated_start = date(current_year, 10, 7)
        
        days_until = (estimated_start - self.current_date).days
        
        return {
            'days_until_season': max(0, days_until),
            'season_start_date': estimated_start,
            'season_started': days_until <= 0,
            'season_year': self._get_season_year_string(estimated_start),
            'estimated': True
        }
    
    def _calculate_estimated_playoff_start(self) -> Dict[str, Any]:
        """Fallback calculation for playoff start when API data unavailable"""
        current_year = self.current_date.year
        current_month = self.current_date.month
        
        # NHL playoffs typically start in mid-April
        if current_month >= 5:
            # If we're past April, playoffs start next year
            estimated_start = date(current_year + 1, 4, 20)
        else:
            # Playoffs might start this year
            estimated_start = date(current_year, 4, 20)
        
        days_until = (estimated_start - self.current_date).days
        
        return {
            'days_until_playoffs': max(0, days_until),
            'playoff_start_date': estimated_start,
            'playoffs_started': days_until <= 0,
            'playoff_status': 'estimated',
            'estimated': True
        }
    
    def _get_season_year_string(self, season_start: date) -> str:
        """Get season year string (e.g., '2024-25')"""
        start_year = season_start.year
        end_year = start_year + 1
        return f"{start_year}-{str(end_year)[-2:]}"
    
    def _get_playoff_status(self) -> str:
        """Determine current playoff status"""
        if not self.playoff_data:
            return "unknown"
        
        # Check if playoffs are active based on rounds data
        rounds = self.playoff_data.get('rounds', {})
        if rounds:
            current_round = self.playoff_data.get('defaultRound', 1)
            if str(current_round) in rounds:
                round_data = rounds[str(current_round)]
                series_list = round_data.get('series', [])
                if series_list:
                    return f"round_{current_round}"
        
        return "not_started"
    
    def get_next_season_info(self) -> Dict[str, Any]:
        """Get information about the next season"""
        season_info = self.get_season_countdown()
        playoff_info = self.get_playoff_countdown()
        
        return {
            'season_countdown': season_info,
            'playoff_countdown': playoff_info,
            'is_offseason': self.is_offseason(),
            'is_playoff_season': self.is_playoff_season(),
            'current_date': self.current_date.isoformat()
        }
