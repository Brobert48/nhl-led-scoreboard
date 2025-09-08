"""
Renderer adapter for Backend v2

Provides compatibility interface between Backend v2 and the existing
rendering system. Ensures the existing renderer receives data in the
exact format it expects without requiring any renderer code changes.
"""

import json
import logging
import threading
import time
import queue
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass
from pathlib import Path

from .config import Backend2Config
from .poller import AdaptivePoller

logger = logging.getLogger(__name__)


@dataclass
class DataUpdate:
    """Represents a data update for the renderer"""
    domain: str
    data: Dict[str, Any]
    timestamp: float
    source_name: str


class RendererAdapter:
    """Adapter that provides data to existing renderer in expected format"""
    
    def __init__(self, config: Backend2Config, poller: AdaptivePoller):
        self.config = config
        self.poller = poller
        
        # Current data state that renderer will access
        self.current_data = {
            'games': [],
            'gameDate': '',
            'teams_info': {},
            'standings': [],
            'overview': {},
            'network_issues': False
        }
        
        # Thread-safe data access
        self.data_lock = threading.RLock()
        
        # Data update queue for threaded access
        self.update_queue = queue.Queue()
        
        # Data freshness tracking
        self.last_updates = {}
        
        # Register callbacks with poller
        self._register_callbacks()
        
        # Compatibility interface for existing Data class
        self.data_interface = BackendDataInterface(self)
    
    def _register_callbacks(self):
        """Register callbacks with poller for data updates"""
        self.poller.register_data_callback('live_game', self._handle_game_update)
        self.poller.register_data_callback('team_info', self._handle_team_info_update)
        self.poller.register_data_callback('standings', self._handle_standings_update)
        self.poller.register_data_callback('schedule', self._handle_schedule_update)
        self.poller.register_data_callback('player_stats', self._handle_player_stats_update)
        self.poller.register_data_callback('playoffs', self._handle_playoff_update)
        self.poller.register_data_callback('season_schedule', self._handle_season_update)
    
    async def _handle_game_update(self, domain: str, data: Dict[str, Any]):
        """Handle live game data updates"""
        with self.data_lock:
            # Update games data in expected format
            self.current_data['games'] = data.get('games', [])
            self.current_data['gameDate'] = data.get('gameDate', '')
            
            # If we have games, update overview with the first preferred game
            if self.current_data['games']:
                preferred_game = self._find_preferred_game()
                if preferred_game:
                    self.current_data['overview'] = preferred_game
            
            self.last_updates[domain] = time.time()
            self.current_data['network_issues'] = False
            
        logger.debug(f"Updated game data: {len(self.current_data['games'])} games")
    
    async def _handle_team_info_update(self, domain: str, data: Dict[str, Any]):
        """Handle team info updates"""
        with self.data_lock:
            # Convert team data to expected format
            teams_data = data.get('data', [])
            
            teams_info = {}
            for team in teams_data:
                team_id = team.get('id')
                if team_id:
                    # Create team info structure expected by renderer
                    teams_info[team_id] = MockTeamInfo(
                        team_id=team_id,
                        name=team.get('fullName', team.get('teamName', '')),
                        abbrev=team.get('triCode', team.get('abbreviation', '')),
                        details=MockTeamDetails(
                            abbrev=team.get('triCode', team.get('abbreviation', '')),
                            name=team.get('fullName', team.get('teamName', ''))
                        )
                    )
            
            self.current_data['teams_info'] = teams_info
            self.last_updates[domain] = time.time()
            
        logger.debug(f"Updated team info: {len(teams_info)} teams")
    
    async def _handle_standings_update(self, domain: str, data: Dict[str, Any]):
        """Handle standings updates"""
        with self.data_lock:
            self.current_data['standings'] = data.get('standings', [])
            self.last_updates[domain] = time.time()
            
        logger.debug(f"Updated standings: {len(self.current_data['standings'])} entries")
    
    async def _handle_schedule_update(self, domain: str, data: Dict[str, Any]):
        """Handle schedule updates"""
        with self.data_lock:
            # Schedule data could be merged with games or kept separate
            schedule_games = data.get('games', [])
            # Could be used to enhance game data or provide future schedule
            self.last_updates[domain] = time.time()
            
        logger.debug(f"Updated schedule: {len(schedule_games)} games")
    
    async def _handle_player_stats_update(self, domain: str, data: Dict[str, Any]):
        """Handle player stats updates"""
        with self.data_lock:
            # Player stats might not be directly used by current renderer
            # but could be cached for future use
            self.last_updates[domain] = time.time()
            
        logger.debug("Updated player stats")
    
    async def _handle_playoff_update(self, domain: str, data: Dict[str, Any]):
        """Handle playoff data updates"""
        with self.data_lock:
            self.current_data['playoffs'] = data.get('rounds', {})
            self.current_data['playoff_series'] = data.get('series', [])
            self.last_updates[domain] = time.time()
            
            # Update countdown calculations
            self._update_countdowns()
            
        logger.debug(f"Updated playoff data: {len(self.current_data.get('playoffs', {}))} rounds")

    async def _handle_season_update(self, domain: str, data: Dict[str, Any]):
        """Handle season schedule updates"""
        with self.data_lock:
            self.current_data['season_info'] = data
            self.last_updates[domain] = time.time()
            
            # Update countdown calculations  
            self._update_countdowns()
            
        logger.debug(f"Updated season info: {data.get('seasonId', 'unknown')}")

    def _update_countdowns(self):
        """Update countdown calculations"""
        from .countdown import CountdownCalculator
        
        calculator = CountdownCalculator(
            season_data=self.current_data.get('season_info', {}),
            playoff_data=self.current_data.get('playoffs', {})
        )
        
        self.current_data['season_countdown'] = calculator.get_season_countdown()
        self.current_data['playoff_countdown'] = calculator.get_playoff_countdown()
        self.current_data['next_season_info'] = calculator.get_next_season_info()
    
    def _find_preferred_game(self) -> Optional[Dict[str, Any]]:
        """Find the first preferred team game for overview"""
        if not self.current_data['games']:
            return None
        
        # Convert preferred team names to IDs
        preferred_team_ids = self._get_preferred_team_ids()
        
        # Look for games with preferred teams
        for game in self.current_data['games']:
            away_team_id = game.get('awayTeam', {}).get('id')
            home_team_id = game.get('homeTeam', {}).get('id')
            
            if away_team_id in preferred_team_ids or home_team_id in preferred_team_ids:
                return game
        
        # Fallback to first game
        return self.current_data['games'][0]
    
    def _get_preferred_team_ids(self) -> List[int]:
        """Convert preferred team names to IDs"""
        team_ids = []
        
        for team_name in self.config.preferred_teams:
            # Look up team ID from teams_info
            for team_id, team_info in self.current_data['teams_info'].items():
                if (team_name.lower() in team_info.name.lower() or 
                    team_name.lower() == team_info.abbrev.lower()):
                    team_ids.append(team_id)
                    break
        
        return team_ids
    
    def get_current_data(self) -> Dict[str, Any]:
        """Get current data state (thread-safe)"""
        with self.data_lock:
            return self.current_data.copy()
    
    def is_data_fresh(self, domain: str, max_age_seconds: int = 300) -> bool:
        """Check if data for domain is fresh"""
        last_update = self.last_updates.get(domain, 0)
        return (time.time() - last_update) < max_age_seconds
    
    def get_network_status(self) -> bool:
        """Check if we have network issues"""
        # Consider data stale if any critical domain hasn't updated recently
        critical_domains = ['live_game', 'team_info']
        
        for domain in critical_domains:
            if not self.is_data_fresh(domain, max_age_seconds=600):  # 10 minutes
                return True  # Network issues
        
        return False
    
    def write_data_to_file(self, file_path: str):
        """Write current data to file for external consumption"""
        try:
            data_copy = self.get_current_data()
            
            # Add metadata
            data_copy['_backend_v2_meta'] = {
                'timestamp': time.time(),
                'last_updates': self.last_updates.copy(),
                'source': 'nhl-scoreboard-backend-v2'
            }
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data_copy, f, indent=2, default=str)
                
        except Exception as e:
            logger.error(f"Failed to write data to file {file_path}: {e}")


class MockTeamInfo:
    """Mock team info object that matches expected interface"""
    
    def __init__(self, team_id: int, name: str, abbrev: str, details=None):
        self.id = team_id
        self.name = name
        self.abbrev = abbrev
        self.details = details or MockTeamDetails(abbrev, name)


class MockTeamDetails:
    """Mock team details object"""
    
    def __init__(self, abbrev: str, name: str):
        self.abbrev = abbrev
        self.name = name
        self.previous_game = None
        self.next_game = None


class BackendDataInterface:
    """Compatibility interface that mimics the existing Data class"""
    
    def __init__(self, adapter: RendererAdapter):
        self.adapter = adapter
        
        # Properties that existing code expects
        self.config = adapter.config
        self.status = MockStatus()
        
        # Additional properties needed by renderer
        self.newUpdate = False  # Backend v2 doesn't handle update checking
        self.needs_refresh = False  # Always false - Backend v2 handles refresh
        self.pb_trigger = False
        self.pb_state = None
        self.curr_board = None
        self.prev_board = None
        self.latlng = None
        self.latlng_msg = ""
        
    @property
    def games(self) -> List[Dict[str, Any]]:
        """Current games list"""
        return self.adapter.current_data.get('games', [])
    
    @property
    def pref_games(self) -> List[Dict[str, Any]]:
        """Preferred team games"""
        all_games = self.games
        preferred_team_ids = self.adapter._get_preferred_team_ids()
        
        pref_games = []
        for game in all_games:
            away_id = game.get('awayTeam', {}).get('id')
            home_id = game.get('homeTeam', {}).get('id')
            
            if away_id in preferred_team_ids or home_id in preferred_team_ids:
                pref_games.append(game)
        
        return pref_games
    
    @property
    def overview(self) -> Dict[str, Any]:
        """Current game overview"""
        return self.adapter.current_data.get('overview', {})
    
    @property
    def teams_info(self) -> Dict[int, MockTeamInfo]:
        """Teams information"""
        return self.adapter.current_data.get('teams_info', {})
    
    @property
    def network_issues(self) -> bool:
        """Network status"""
        return self.adapter.get_network_status()
    
    def refresh_data(self):
        """Refresh data (no-op in Backend v2 - polling handles this)"""
        pass
    
    def refresh_overview(self):
        """Refresh overview (no-op in Backend v2)"""
        pass
    
    def refresh_games(self):
        """Refresh games (no-op in Backend v2)"""
        pass
    
    def is_pref_team_offday(self) -> bool:
        """Check if preferred teams have no games today"""
        return len(self.pref_games) == 0
    
    @property
    def season_countdown(self) -> Dict[str, Any]:
        """Season countdown information"""
        return self.adapter.current_data.get('season_countdown', {})

    @property
    def playoff_countdown(self) -> Dict[str, Any]:
        """Playoff countdown information"""
        return self.adapter.current_data.get('playoff_countdown', {})

    @property
    def playoffs(self) -> Dict[str, Any]:
        """Playoff tournament data"""
        return self.adapter.current_data.get('playoffs', {})
    
    @property
    def next_season_info(self) -> Dict[str, Any]:
        """Next season information"""
        return self.adapter.current_data.get('next_season_info', {})
    
    def other_games(self) -> List[Dict[str, Any]]:
        """Get list of other (non-preferred) games for scoreticker"""
        all_games = self.games
        preferred_team_ids = self.adapter._get_preferred_team_ids()
        
        other_games = []
        for game in all_games:
            away_id = game.get('awayTeam', {}).get('id')
            home_id = game.get('homeTeam', {}).get('id')
            
            # Include games that don't involve preferred teams
            if away_id not in preferred_team_ids and home_id not in preferred_team_ids:
                other_games.append(game)
        
        return other_games


class MockStatus:
    """Mock status object that provides game state checking"""
    
    def is_live(self, game_state: str) -> bool:
        """Check if game is live"""
        return game_state in ['LIVE', 'CRIT']
    
    def is_scheduled(self, game_state: str) -> bool:
        """Check if game is scheduled"""
        return game_state in ['FUT', 'PRE']
    
    def is_final(self, game_state: str) -> bool:
        """Check if game is final"""
        return game_state in ['FINAL', 'OFF']
    
    def is_game_over(self, game_state: str) -> bool:
        """Check if game is over"""
        return game_state in ['FINAL', 'OFF']
    
    def is_irregular(self, game_state: str) -> bool:
        """Check if game is in irregular state"""
        return game_state in ['PPD', 'SUSP', 'CAN']


def create_renderer_adapter(config: Backend2Config, poller: AdaptivePoller) -> RendererAdapter:
    """Create and initialize renderer adapter"""
    return RendererAdapter(config, poller)
