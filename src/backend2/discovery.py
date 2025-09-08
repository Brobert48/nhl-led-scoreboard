"""
Data source discovery and validation for Backend v2

Handles:
- Finding relevant API endpoints for each data domain
- Parsing HTML pages for JSON API endpoints
- Validating endpoints by fetching and analyzing response structure
- Maintaining a registry of active endpoints with their schemas
"""

import requests
import json
import re
import logging
import time
from urllib.parse import urljoin, urlparse
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass, asdict

try:
    from bs4 import BeautifulSoup
except ImportError:
    # Fallback if BeautifulSoup is not available
    BeautifulSoup = None

from .config import Backend2Config, DataSourceConfig
from .cache import CacheManager, create_schema_fingerprint

logger = logging.getLogger(__name__)


@dataclass
class DiscoveredEndpoint:
    """Represents a discovered API endpoint"""
    url: str
    domain: str
    source_name: str
    method: str = "GET"
    requires_params: bool = False
    sample_params: Dict[str, str] = None
    response_format: str = "json"
    last_validated: float = 0
    validation_success: bool = False
    expected_keys: List[str] = None
    schema_fingerprint_hash: Optional[str] = None
    discovery_method: str = "static"  # static, html_parse, api_discovery
    
    def __post_init__(self):
        if self.sample_params is None:
            self.sample_params = {}
        if self.expected_keys is None:
            self.expected_keys = []


class DataSourceDiscovery:
    """Handles discovery and validation of data sources for each domain"""
    
    def __init__(self, config: Backend2Config, cache_manager: CacheManager):
        self.config = config
        self.cache = cache_manager
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'NHL-LED-Scoreboard/2.0 (compatible; data discovery)'
        })
        
        # Registry of discovered endpoints by domain
        self.endpoints: Dict[str, List[DiscoveredEndpoint]] = {}
        
        # Domain-specific validation patterns
        self.domain_patterns = {
            'live_game': {
                'required_keys': ['games', 'gameDate'],
                'game_keys': ['awayTeam', 'homeTeam', 'gameState', 'gameDate'],
                'team_keys': ['id', 'name', 'score'],
                'sample_endpoints': [
                    '/score/{date}',
                    '/gamecenter/{game_id}/play-by-play'
                ]
            },
            'standings': {
                'required_keys': ['standings'],
                'standing_keys': ['teamName', 'wins', 'losses'],
                'sample_endpoints': [
                    '/standings',
                    '/standings/wildCardWithLeaders'
                ]
            },
            'team_info': {
                'required_keys': ['data'],
                'team_keys': ['id', 'triCode', 'fullName'],
                'sample_endpoints': [
                    '/teams',
                    '/stats/rest/en/team'
                ]
            },
            'schedule': {
                'required_keys': ['games'],
                'schedule_keys': ['gameDate', 'awayTeam', 'homeTeam'],
                'sample_endpoints': [
                    '/schedule',
                    '/club-schedule-season/{team_abbrev}/{season}'
                ]
            },
            'player_stats': {
                'required_keys': ['people'],
                'player_keys': ['id', 'fullName', 'stats'],
                'sample_endpoints': [
                    '/people/{player_id}',
                    '/stats/rest/en/skater'
                ]
            },
            'playoffs': {
                'required_keys': ['rounds'],
                'tournament_keys': ['id', 'season', 'rounds'],
                'series_keys': ['seriesLetter', 'matchupTeams', 'currentGame'],
                'sample_endpoints': [
                    '/tournaments/playoffs',
                    '/playoffs/{season}',
                    '/tournaments/{tournament_id}',
                    '/standings-season'
                ]
            },
            'season_schedule': {
                'required_keys': ['seasonId'],
                'season_keys': ['seasonId', 'startDate', 'endDate'],
                'sample_endpoints': [
                    '/seasons/current',
                    '/seasons/{season_id}',
                    '/schedule-calendar/{season}',
                    '/season'
                ]
            }
        }
    
    def discover_all_sources(self) -> Dict[str, List[DiscoveredEndpoint]]:
        """Discover and validate all data sources for all domains"""
        logger.info("Starting data source discovery...")
        
        for domain in self.domain_patterns.keys():
            logger.info(f"Discovering sources for domain: {domain}")
            self.endpoints[domain] = []
            
            # Get configured sources for this domain
            sources = self.config.get_all_sources_for_domain(domain)
            
            for source in sources:
                if not source.enabled:
                    continue
                
                try:
                    discovered = self._discover_domain_endpoints(domain, source)
                    self.endpoints[domain].extend(discovered)
                    logger.info(f"Discovered {len(discovered)} endpoints for {source.name}/{domain}")
                except Exception as e:
                    logger.error(f"Failed to discover endpoints for {source.name}/{domain}: {e}")
        
        # Cache the discovery results
        self._cache_discovery_results()
        
        logger.info(f"Discovery complete. Found endpoints for {len(self.endpoints)} domains")
        return self.endpoints
    
    def _discover_domain_endpoints(self, domain: str, source: DataSourceConfig) -> List[DiscoveredEndpoint]:
        """Discover endpoints for a specific domain and source"""
        discovered = []
        
        if source.base_url.startswith('file://'):
            # Handle local file sources
            endpoint = DiscoveredEndpoint(
                url=source.base_url,
                domain=domain,
                source_name=source.name,
                discovery_method="static"
            )
            if self._validate_endpoint(endpoint, source):
                discovered.append(endpoint)
            return discovered
        
        # Try static endpoint discovery first
        static_endpoints = self._discover_static_endpoints(domain, source)
        discovered.extend(static_endpoints)
        
        # Try HTML parsing for additional endpoints
        if len(static_endpoints) == 0:
            html_endpoints = self._discover_from_html(domain, source)
            discovered.extend(html_endpoints)
        
        # For initial discovery, don't validate endpoints with network calls
        # Validation will happen during actual polling
        for endpoint in discovered:
            endpoint.validation_success = True  # Mark as valid for discovery
            endpoint.last_validated = time.time()
        
        logger.debug(f"Returning {len(discovered)} discovered endpoints for {domain}/{source.name}")
        return discovered
    
    def _discover_static_endpoints(self, domain: str, source: DataSourceConfig) -> List[DiscoveredEndpoint]:
        """Discover endpoints using known static patterns"""
        endpoints = []
        
        if domain not in self.domain_patterns:
            return endpoints
        
        sample_endpoints = self.domain_patterns[domain].get('sample_endpoints', [])
        
        for endpoint_pattern in sample_endpoints:
            # Create endpoint with sample parameters
            url = urljoin(source.base_url, endpoint_pattern)
            
            endpoint = DiscoveredEndpoint(
                url=url,
                domain=domain,
                source_name=source.name,
                requires_params='{' in endpoint_pattern,
                sample_params=self._extract_sample_params(endpoint_pattern, domain),
                discovery_method="static"
            )
            
            endpoints.append(endpoint)
        
        return endpoints
    
    def _extract_sample_params(self, endpoint_pattern: str, domain: str) -> Dict[str, str]:
        """Extract sample parameters from endpoint pattern"""
        params = {}
        
        # Extract parameter placeholders
        param_matches = re.findall(r'\{([^}]+)\}', endpoint_pattern)
        
        for param in param_matches:
            if param == 'date':
                params['date'] = time.strftime('%Y-%m-%d')
            elif param == 'game_id':
                params['game_id'] = '2023020001'  # Sample game ID
            elif param == 'player_id':
                params['player_id'] = '8478402'  # Sample player ID (McDavid)
            elif param == 'team_abbrev':
                if self.config.preferred_teams:
                    # Try to map team name to abbreviation
                    team_name = self.config.preferred_teams[0]
                    params['team_abbrev'] = self._team_name_to_abbrev(team_name)
                else:
                    params['team_abbrev'] = 'TOR'
            elif param == 'season':
                current_year = time.gmtime().tm_year
                params['season'] = f"{current_year}{current_year + 1}"
        
        return params
    
    def _team_name_to_abbrev(self, team_name: str) -> str:
        """Convert team name to abbreviation"""
        team_mappings = {
            'Toronto Maple Leafs': 'TOR',
            'Montreal Canadiens': 'MTL',
            'Boston Bruins': 'BOS',
            'Tampa Bay Lightning': 'TBL',
            'Florida Panthers': 'FLA',
            'Detroit Red Wings': 'DET',
            'Buffalo Sabres': 'BUF',
            'Ottawa Senators': 'OTT',
            'New York Rangers': 'NYR',
            'New York Islanders': 'NYI',
            'New Jersey Devils': 'NJD',
            'Philadelphia Flyers': 'PHI',
            'Pittsburgh Penguins': 'PIT',
            'Washington Capitals': 'WSH',
            'Carolina Hurricanes': 'CAR',
            'Columbus Blue Jackets': 'CBJ',
            'Nashville Predators': 'NSH',
            # Add more as needed...
        }
        
        return team_mappings.get(team_name, 'TOR')
    
    def _discover_from_html(self, domain: str, source: DataSourceConfig) -> List[DiscoveredEndpoint]:
        """Discover endpoints by parsing HTML pages for API calls"""
        endpoints = []
        
        if BeautifulSoup is None:
            logger.warning("BeautifulSoup not available, skipping HTML parsing")
            return endpoints
        
        try:
            # Fetch the main page
            response = self.session.get(source.base_url, timeout=source.timeout_seconds)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for script tags with API calls
            scripts = soup.find_all('script')
            
            for script in scripts:
                if script.string:
                    # Find potential API URLs
                    api_urls = re.findall(r'["\']([^"\']*api[^"\']*\.json?[^"\']*)["\']', script.string)
                    
                    for url in api_urls:
                        if self._is_relevant_endpoint(url, domain):
                            endpoint = DiscoveredEndpoint(
                                url=urljoin(source.base_url, url),
                                domain=domain,
                                source_name=source.name,
                                discovery_method="html_parse"
                            )
                            endpoints.append(endpoint)
            
            # Look for data attributes that might contain API URLs
            elements_with_data = soup.find_all(attrs={'data-url': True})
            for element in elements_with_data:
                url = element.get('data-url')
                if url and self._is_relevant_endpoint(url, domain):
                    endpoint = DiscoveredEndpoint(
                        url=urljoin(source.base_url, url),
                        domain=domain,
                        source_name=source.name,
                        discovery_method="html_parse"
                    )
                    endpoints.append(endpoint)
        
        except Exception as e:
            logger.warning(f"Failed to parse HTML for {source.name}: {e}")
        
        return endpoints
    
    def _is_relevant_endpoint(self, url: str, domain: str) -> bool:
        """Check if discovered URL is relevant to the domain"""
        url_lower = url.lower()
        
        domain_keywords = {
            'live_game': ['game', 'score', 'live', 'play-by-play'],
            'standings': ['standing', 'ranking', 'table'],
            'team_info': ['team', 'roster', 'info'],
            'schedule': ['schedule', 'calendar', 'upcoming'],
            'player_stats': ['player', 'stats', 'people']
        }
        
        keywords = domain_keywords.get(domain, [])
        return any(keyword in url_lower for keyword in keywords)
    
    def _validate_endpoint(self, endpoint: DiscoveredEndpoint, source: DataSourceConfig) -> bool:
        """Validate an endpoint by testing it and checking response structure"""
        try:
            # Handle file:// URLs
            if endpoint.url.startswith('file://'):
                return self._validate_file_endpoint(endpoint)
            
            # Build URL with sample parameters if needed
            test_url = endpoint.url
            if endpoint.requires_params and endpoint.sample_params:
                test_url = endpoint.url.format(**endpoint.sample_params)
            
            # Make test request
            response = self.session.get(
                test_url,
                timeout=source.timeout_seconds,
                headers={'Accept': 'application/json'}
            )
            
            if response.status_code != 200:
                logger.debug(f"Endpoint validation failed with status {response.status_code}: {test_url}")
                return False
            
            # Try to parse as JSON
            try:
                data = response.json()
            except json.JSONDecodeError:
                logger.debug(f"Endpoint returned non-JSON data: {test_url}")
                return False
            
            # Validate structure against domain patterns
            if not self._validate_response_structure(data, endpoint.domain):
                logger.debug(f"Endpoint response structure validation failed: {test_url}")
                return False
            
            # Update endpoint with validation results
            endpoint.last_validated = time.time()
            endpoint.validation_success = True
            endpoint.expected_keys = self._extract_expected_keys(data, endpoint.domain)
            
            # Create and cache schema fingerprint
            fingerprint = create_schema_fingerprint(data, endpoint.source_name, endpoint.domain)
            endpoint.schema_fingerprint_hash = fingerprint.version_hash
            self.cache.schema_cache.set_fingerprint(fingerprint)
            
            logger.debug(f"Successfully validated endpoint: {test_url}")
            return True
            
        except Exception as e:
            logger.debug(f"Endpoint validation failed: {endpoint.url} - {e}")
            return False
    
    def _validate_file_endpoint(self, endpoint: DiscoveredEndpoint) -> bool:
        """Validate a local file endpoint"""
        try:
            file_path = endpoint.url.replace('file://', '')
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if self._validate_response_structure(data, endpoint.domain):
                endpoint.last_validated = time.time()
                endpoint.validation_success = True
                endpoint.expected_keys = self._extract_expected_keys(data, endpoint.domain)
                return True
            
        except Exception as e:
            logger.debug(f"File endpoint validation failed: {endpoint.url} - {e}")
        
        return False
    
    def _validate_response_structure(self, data: Dict[str, Any], domain: str) -> bool:
        """Validate response structure against domain patterns"""
        if domain not in self.domain_patterns:
            return True  # No specific validation pattern
        
        patterns = self.domain_patterns[domain]
        required_keys = patterns.get('required_keys', [])
        
        # Check for required top-level keys
        for key in required_keys:
            if key not in data:
                return False
        
        # Domain-specific validation
        if domain == 'live_game':
            return self._validate_game_data(data)
        elif domain == 'standings':
            return self._validate_standings_data(data)
        elif domain == 'team_info':
            return self._validate_team_data(data)
        elif domain == 'schedule':
            return self._validate_schedule_data(data)
        elif domain == 'player_stats':
            return self._validate_player_data(data)
        
        return True
    
    def _validate_game_data(self, data: Dict[str, Any]) -> bool:
        """Validate game data structure"""
        if 'games' not in data:
            return False
        
        games = data['games']
        if not isinstance(games, list) or len(games) == 0:
            return True  # Empty games list is valid
        
        # Check first game structure
        game = games[0]
        required_game_keys = ['awayTeam', 'homeTeam', 'gameState']
        
        for key in required_game_keys:
            if key not in game:
                return False
        
        # Check team structure
        for team_key in ['awayTeam', 'homeTeam']:
            team = game[team_key]
            if not isinstance(team, dict) or 'id' not in team:
                return False
        
        return True
    
    def _validate_standings_data(self, data: Dict[str, Any]) -> bool:
        """Validate standings data structure"""
        if 'standings' not in data:
            return False
        
        standings = data['standings']
        if not isinstance(standings, list) or len(standings) == 0:
            return True
        
        # Check first standing entry
        standing = standings[0]
        return isinstance(standing, dict) and 'teamName' in standing
    
    def _validate_team_data(self, data: Dict[str, Any]) -> bool:
        """Validate team data structure"""
        # Handle different team data formats
        if 'data' in data:
            teams = data['data']
        elif 'teams' in data:
            teams = data['teams']
        else:
            return False
        
        if not isinstance(teams, list) or len(teams) == 0:
            return True
        
        team = teams[0]
        return isinstance(team, dict) and ('id' in team or 'triCode' in team)
    
    def _validate_schedule_data(self, data: Dict[str, Any]) -> bool:
        """Validate schedule data structure"""
        if 'games' not in data:
            return False
        
        games = data['games']
        if not isinstance(games, list) or len(games) == 0:
            return True
        
        game = games[0]
        required_keys = ['gameDate', 'awayTeam', 'homeTeam']
        return all(key in game for key in required_keys)
    
    def _validate_player_data(self, data: Dict[str, Any]) -> bool:
        """Validate player data structure"""
        # Handle different player data formats
        if 'people' in data:
            players = data['people']
        elif 'data' in data:
            players = data['data']
        else:
            return False
        
        if not isinstance(players, list) or len(players) == 0:
            return True
        
        player = players[0]
        return isinstance(player, dict) and 'id' in player
    
    def _extract_expected_keys(self, data: Dict[str, Any], domain: str) -> List[str]:
        """Extract list of expected keys from validated response"""
        keys = []
        
        def extract_keys(obj: Any, path: str = ""):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    current_path = f"{path}.{key}" if path else key
                    keys.append(current_path)
                    if isinstance(value, (dict, list)) and len(keys) < 50:  # Limit depth
                        extract_keys(value, current_path)
            elif isinstance(obj, list) and obj and len(keys) < 50:
                extract_keys(obj[0], f"{path}[0]")
        
        extract_keys(data)
        return keys[:50]  # Limit to first 50 keys
    
    def _cache_discovery_results(self):
        """Cache discovery results for faster startup"""
        cache_key = "discovery_results"
        
        # Convert endpoints to serializable format
        serializable_endpoints = {}
        for domain, endpoints in self.endpoints.items():
            serializable_endpoints[domain] = [asdict(ep) for ep in endpoints]
        
        self.cache.set(cache_key, serializable_endpoints, ttl_seconds=86400)  # Cache for 24 hours
    
    def load_cached_discovery_results(self) -> bool:
        """Load discovery results from cache"""
        cache_key = "discovery_results"
        cached_entry = self.cache.get(cache_key)
        
        if not cached_entry:
            return False
        
        try:
            serialized_endpoints = cached_entry.data
            self.endpoints = {}
            
            for domain, endpoints_data in serialized_endpoints.items():
                self.endpoints[domain] = [
                    DiscoveredEndpoint(**ep_data) for ep_data in endpoints_data
                ]
            
            logger.info(f"Loaded cached discovery results for {len(self.endpoints)} domains")
            return True
            
        except Exception as e:
            logger.warning(f"Failed to load cached discovery results: {e}")
            return False
    
    def get_active_endpoints(self, domain: str) -> List[DiscoveredEndpoint]:
        """Get list of active validated endpoints for a domain"""
        if domain not in self.endpoints:
            return []
        
        # Return only validated endpoints, sorted by discovery method priority
        validated = [ep for ep in self.endpoints[domain] if ep.validation_success]
        
        # Prioritize static discovery over HTML parsing
        priority_order = {'static': 1, 'html_parse': 2, 'api_discovery': 3}
        
        return sorted(validated, key=lambda x: priority_order.get(x.discovery_method, 99))
    
    def refresh_endpoint_validation(self, domain: str, force: bool = False):
        """Refresh validation for endpoints in a domain"""
        if domain not in self.endpoints:
            return
        
        current_time = time.time()
        validation_interval = 3600  # Re-validate every hour
        
        for endpoint in self.endpoints[domain]:
            if force or (current_time - endpoint.last_validated) > validation_interval:
                source = self.config.get_source_for_domain(domain)
                if source:
                    self._validate_endpoint(endpoint, source)


def run_discovery(config: Backend2Config, cache_manager: CacheManager) -> Dict[str, List[DiscoveredEndpoint]]:
    """Run complete data source discovery process"""
    discovery = DataSourceDiscovery(config, cache_manager)
    
    # Try to load cached results first
    cache_loaded = discovery.load_cached_discovery_results()
    
    # Check if cached results are actually useful (non-empty)
    total_cached_endpoints = sum(len(eps) for eps in discovery.endpoints.values()) if cache_loaded else 0
    
    if not cache_loaded or total_cached_endpoints == 0:
        # No cache or empty cache, run full discovery
        logger.info("Running fresh discovery (no cache or empty cache)")
        discovery.discover_all_sources()
    else:
        # Refresh validation for cached results
        logger.info(f"Using cached discovery results ({total_cached_endpoints} endpoints)")
        for domain in discovery.endpoints.keys():
            discovery.refresh_endpoint_validation(domain)
    
    return discovery.endpoints
