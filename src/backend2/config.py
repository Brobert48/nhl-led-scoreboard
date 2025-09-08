"""
Configuration loader for Backend v2

Handles loading and parsing of YAML/JSON configuration files for:
- Team preferences and timezone settings
- Polling intervals and cache paths
- Data source priorities and fallback configuration
"""

import json
import yaml
import os
import logging
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from datetime import timezone, timedelta

logger = logging.getLogger(__name__)


@dataclass
class PollingConfig:
    """Configuration for adaptive polling intervals"""
    live_game_fast: int = 3  # seconds during active play
    live_game_slow: int = 20  # seconds during intermission
    pregame: int = 60  # seconds before game starts
    postgame: int = 300  # seconds after game ends
    player_stats: int = 900  # seconds for player/team stats
    standings: int = 1800  # seconds for standings
    schedule: int = 3600  # seconds for schedule updates
    offline: int = 14400  # seconds when no games
    playoffs: int = 3600  # seconds for playoff data (1 hour during off-season)
    season_schedule: int = 86400  # seconds for season dates (24 hours)


@dataclass
class CacheConfig:
    """Configuration for caching behavior"""
    base_path: str = "/tmp/nhl-scoreboard-cache"
    json_cache: bool = True
    sqlite_cache: bool = False
    max_age_seconds: int = 3600
    schema_cache_age: int = 86400  # 24 hours
    cleanup_interval: int = 3600


@dataclass
class DataSourceConfig:
    """Configuration for a single data source"""
    name: str
    base_url: str
    priority: int = 1
    enabled: bool = True
    requires_api_key: bool = False
    api_key: Optional[str] = None
    timeout_seconds: int = 10
    rate_limit_per_minute: int = 60
    user_agent: str = "NHL-LED-Scoreboard/2.0"


@dataclass
class Backend2Config:
    """Main configuration class for Backend v2"""
    # Team and display preferences
    preferred_teams: List[str] = field(default_factory=list)
    timezone: str = "America/New_York"
    time_format: str = "12h"
    
    # Polling configuration
    polling: PollingConfig = field(default_factory=PollingConfig)
    
    # Cache configuration
    cache: CacheConfig = field(default_factory=CacheConfig)
    
    # Data sources in priority order
    data_sources: Dict[str, Dict[str, DataSourceConfig]] = field(default_factory=dict)
    
    # Logging
    log_level: str = "INFO"
    log_file: Optional[str] = None
    
    # Pi-specific optimizations
    pi_optimizations: bool = True
    max_concurrent_requests: int = 5
    
    def __post_init__(self):
        """Initialize default data sources if none provided"""
        if not self.data_sources:
            self._setup_default_sources()
    
    def _setup_default_sources(self):
        """Setup default NHL API data sources with fallbacks"""
        self.data_sources = {
            "live_game": {
                "nhl_official": DataSourceConfig(
                    name="NHL Official API",
                    base_url="https://api-web.nhle.com/v1",
                    priority=1,
                    enabled=True
                ),
                "nhl_legacy": DataSourceConfig(
                    name="NHL Legacy API",
                    base_url="https://api.nhle.com/stats/rest/en",
                    priority=2,
                    enabled=True
                ),
                "espn": DataSourceConfig(
                    name="ESPN API",
                    base_url="https://site.api.espn.com/apis/site/v2/sports/hockey/nhl",
                    priority=3,
                    enabled=True
                )
            },
            "standings": {
                "nhl_official": DataSourceConfig(
                    name="NHL Official Standings",
                    base_url="https://api-web.nhle.com/v1",
                    priority=1,
                    enabled=True
                ),
                "espn": DataSourceConfig(
                    name="ESPN Standings",
                    base_url="https://site.api.espn.com/apis/v2/sports/hockey/nhl",
                    priority=2,
                    enabled=True
                )
            },
            "team_info": {
                "nhl_official": DataSourceConfig(
                    name="NHL Official Teams",
                    base_url="https://api-web.nhle.com/v1",
                    priority=1,
                    enabled=True
                ),
                "backup_json": DataSourceConfig(
                    name="Local Backup",
                    base_url="file://src/data/backup_teams_data.json",
                    priority=99,
                    enabled=True
                )
            },
            "schedule": {
                "nhl_official": DataSourceConfig(
                    name="NHL Official Schedule",
                    base_url="https://api-web.nhle.com/v1",
                    priority=1,
                    enabled=True
                ),
                "espn": DataSourceConfig(
                    name="ESPN Schedule", 
                    base_url="https://site.api.espn.com/apis/v2/sports/hockey/nhl",
                    priority=2,
                    enabled=True
                )
            },
            "player_stats": {
                "nhl_official": DataSourceConfig(
                    name="NHL Official Stats",
                    base_url="https://api-web.nhle.com/v1",
                    priority=1,
                    enabled=True
                )
            }
        }
    
    @classmethod
    def load_from_file(cls, config_path: str) -> 'Backend2Config':
        """Load configuration from YAML or JSON file"""
        path = Path(config_path)
        
        if not path.exists():
            logger.warning(f"Config file {config_path} not found, using defaults")
            return cls()
        
        with open(path, 'r', encoding='utf-8') as f:
            if path.suffix.lower() in ['.yaml', '.yml']:
                config_data = yaml.safe_load(f)
            else:
                config_data = json.load(f)
        
        return cls.from_dict(config_data)
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'Backend2Config':
        """Create configuration from dictionary"""
        # Extract nested configurations
        polling_config = PollingConfig(**config_dict.get('polling', {}))
        cache_config = CacheConfig(**config_dict.get('cache', {}))
        
        # Parse data sources
        data_sources = {}
        for domain, sources in config_dict.get('data_sources', {}).items():
            data_sources[domain] = {}
            for source_key, source_data in sources.items():
                data_sources[domain][source_key] = DataSourceConfig(**source_data)
        
        # Create main config
        main_config = {k: v for k, v in config_dict.items() 
                      if k not in ['polling', 'cache', 'data_sources']}
        main_config.update({
            'polling': polling_config,
            'cache': cache_config,
            'data_sources': data_sources
        })
        
        return cls(**main_config)
    
    def get_timezone(self):
        """Get timezone object"""
        if self.timezone.startswith('UTC'):
            offset_str = self.timezone[3:]
            if offset_str:
                hours = int(offset_str)
                return timezone(timedelta(hours=hours))
            return timezone.utc
        
        # For named timezones, we'd need pytz, but for Pi compatibility
        # we'll stick with simple UTC offsets and EST/PST shortcuts
        timezone_map = {
            'America/New_York': timezone(timedelta(hours=-5)),
            'America/Toronto': timezone(timedelta(hours=-5)),
            'America/Chicago': timezone(timedelta(hours=-6)),
            'America/Denver': timezone(timedelta(hours=-7)),
            'America/Los_Angeles': timezone(timedelta(hours=-8)),
            'America/Vancouver': timezone(timedelta(hours=-8)),
        }
        
        return timezone_map.get(self.timezone, timezone.utc)
    
    def get_source_for_domain(self, domain: str, priority: int = 1) -> Optional[DataSourceConfig]:
        """Get the data source for a domain by priority"""
        if domain not in self.data_sources:
            return None
        
        sources = self.data_sources[domain]
        for source in sorted(sources.values(), key=lambda x: x.priority):
            if source.enabled and source.priority >= priority:
                return source
        
        return None
    
    def get_all_sources_for_domain(self, domain: str) -> List[DataSourceConfig]:
        """Get all enabled sources for a domain, sorted by priority"""
        if domain not in self.data_sources:
            return []
        
        sources = [s for s in self.data_sources[domain].values() if s.enabled]
        return sorted(sources, key=lambda x: x.priority)


def load_config(config_path: str = "config/backend2.yaml") -> Backend2Config:
    """Load Backend v2 configuration from file or use defaults"""
    try:
        return Backend2Config.load_from_file(config_path)
    except Exception as e:
        logger.error(f"Failed to load config from {config_path}: {e}")
        logger.info("Using default configuration")
        return Backend2Config()


def create_sample_config(output_path: str = "config/backend2.yaml"):
    """Create a sample configuration file"""
    config = Backend2Config()
    
    # Convert to dictionary for serialization
    config_dict = {
        'preferred_teams': ['Toronto Maple Leafs', 'Montreal Canadiens'],
        'timezone': 'America/New_York',
        'time_format': '12h',
        'polling': {
            'live_game_fast': 3,
            'live_game_slow': 20,
            'pregame': 60,
            'postgame': 300,
            'player_stats': 900,
            'standings': 1800,
            'schedule': 3600,
            'offline': 14400
        },
        'cache': {
            'base_path': '/tmp/nhl-scoreboard-cache',
            'json_cache': True,
            'sqlite_cache': False,
            'max_age_seconds': 3600,
            'schema_cache_age': 86400,
            'cleanup_interval': 3600
        },
        'data_sources': {
            'live_game': {
                'nhl_official': {
                    'name': 'NHL Official API',
                    'base_url': 'https://api-web.nhle.com/v1',
                    'priority': 1,
                    'enabled': True,
                    'timeout_seconds': 10
                },
                'espn': {
                    'name': 'ESPN API',
                    'base_url': 'https://site.api.espn.com/apis/site/v2/sports/hockey/nhl',
                    'priority': 2,
                    'enabled': True,
                    'timeout_seconds': 15
                }
            },
            'standings': {
                'nhl_official': {
                    'name': 'NHL Official Standings',
                    'base_url': 'https://api-web.nhle.com/v1',
                    'priority': 1,
                    'enabled': True,
                    'timeout_seconds': 10
                },
                'espn': {
                    'name': 'ESPN Standings',
                    'base_url': 'https://site.api.espn.com/apis/v2/sports/hockey/nhl',
                    'priority': 2,
                    'enabled': True,
                    'timeout_seconds': 15
                }
            },
            'team_info': {
                'nhl_official': {
                    'name': 'NHL Official Teams',
                    'base_url': 'https://api-web.nhle.com/v1',
                    'priority': 1,
                    'enabled': True,
                    'timeout_seconds': 10
                },
                'backup_json': {
                    'name': 'Local Backup Teams',
                    'base_url': 'file://src/data/backup_teams_data.json',
                    'priority': 99,
                    'enabled': True,
                    'timeout_seconds': 5
                }
            },
            'schedule': {
                'nhl_official': {
                    'name': 'NHL Official Schedule',
                    'base_url': 'https://api-web.nhle.com/v1',
                    'priority': 1,
                    'enabled': True,
                    'timeout_seconds': 10
                },
                'espn': {
                    'name': 'ESPN Schedule',
                    'base_url': 'https://site.api.espn.com/apis/v2/sports/hockey/nhl',
                    'priority': 2,
                    'enabled': True,
                    'timeout_seconds': 15
                }
            },
            'player_stats': {
                'nhl_official': {
                    'name': 'NHL Official Stats',
                    'base_url': 'https://api-web.nhle.com/v1',
                    'priority': 1,
                    'enabled': True,
                    'timeout_seconds': 10
                }
            }
        },
        'log_level': 'INFO',
        'pi_optimizations': True,
        'max_concurrent_requests': 5
    }
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)
    
    logger.info(f"Sample configuration created at {output_path}")
