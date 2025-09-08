"""
Adaptive polling system for Backend v2

Handles:
- Polling primary feeds at adaptive rates based on data type and game state
- Automatic fallback to secondary sources on failure
- ETags and Last-Modified headers for efficient polling
- Rate limiting and error handling
- Adaptive intervals based on game state and data freshness
"""

import asyncio
import aiohttp
import json
import logging
import time
import hashlib
from typing import Dict, List, Any, Optional, Tuple, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin
from contextlib import asynccontextmanager

from .config import Backend2Config, DataSourceConfig
from .cache import CacheManager
from .discovery import DiscoveredEndpoint
from .parser import DataParser

logger = logging.getLogger(__name__)


@dataclass
class PollResult:
    """Result of a polling operation"""
    success: bool
    data: Optional[Dict[str, Any]] = None
    source_name: str = ""
    endpoint_url: str = ""
    http_status: int = 0
    cached: bool = False
    error_message: str = ""
    poll_duration_ms: int = 0
    next_poll_interval: int = 60
    etag: Optional[str] = None
    last_modified: Optional[str] = None


@dataclass
class DomainState:
    """State information for a polling domain"""
    domain: str
    last_poll_time: float = 0
    last_successful_poll: float = 0
    current_interval: int = 60
    consecutive_failures: int = 0
    active_source_index: int = 0
    cached_data: Optional[Dict[str, Any]] = None
    game_state: str = "UNKNOWN"
    is_live_game: bool = False
    intermission: bool = False


class AdaptivePoller:
    """Main adaptive polling coordinator"""
    
    def __init__(self, config: Backend2Config, cache_manager: CacheManager, 
                 endpoints: Dict[str, List[DiscoveredEndpoint]], parser: DataParser):
        self.config = config
        self.cache = cache_manager
        self.endpoints = endpoints
        self.parser = parser
        
        # Domain states
        self.domain_states: Dict[str, DomainState] = {}
        for domain in endpoints.keys():
            self.domain_states[domain] = DomainState(domain=domain)
        
        # Polling control
        self.polling_active = False
        self.poll_tasks: Dict[str, asyncio.Task] = {}
        
        # HTTP session with connection pooling
        self.session: Optional[aiohttp.ClientSession] = None
        
        # Rate limiting
        self.rate_limits: Dict[str, List[float]] = {}
        
        # Callbacks for data updates
        self.data_callbacks: Dict[str, List[Callable]] = {}
    
    async def start_polling(self):
        """Start adaptive polling for all domains"""
        if self.polling_active:
            logger.warning("Polling already active")
            return
        
        self.polling_active = True
        
        # Create HTTP session
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        connector = aiohttp.TCPConnector(
            limit=self.config.max_concurrent_requests,
            limit_per_host=3,
            keepalive_timeout=30,
            enable_cleanup_closed=True
        )
        
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={'User-Agent': 'NHL-LED-Scoreboard/2.0'}
        )
        
        # Start polling tasks for each domain
        for domain in self.domain_states.keys():
            task = asyncio.create_task(self._poll_domain_loop(domain))
            self.poll_tasks[domain] = task
        
        logger.info(f"Started adaptive polling for {len(self.domain_states)} domains")
    
    async def stop_polling(self):
        """Stop all polling tasks"""
        if not self.polling_active:
            return
        
        self.polling_active = False
        
        # Cancel all polling tasks
        for task in self.poll_tasks.values():
            task.cancel()
        
        # Wait for tasks to complete
        if self.poll_tasks:
            await asyncio.gather(*self.poll_tasks.values(), return_exceptions=True)
        
        # Close HTTP session
        if self.session:
            await self.session.close()
            self.session = None
        
        self.poll_tasks.clear()
        logger.info("Stopped adaptive polling")
    
    async def _poll_domain_loop(self, domain: str):
        """Main polling loop for a specific domain"""
        state = self.domain_states[domain]
        
        while self.polling_active:
            try:
                # Calculate next poll time
                current_time = time.time()
                next_poll_time = state.last_poll_time + state.current_interval
                
                # Wait until next poll time
                if current_time < next_poll_time:
                    sleep_duration = next_poll_time - current_time
                    await asyncio.sleep(sleep_duration)
                
                # Perform poll
                poll_result = await self._poll_domain(domain)
                
                # Update state based on result
                await self._update_domain_state(domain, poll_result)
                
                # Notify callbacks if we have new data
                if poll_result.success and poll_result.data and not poll_result.cached:
                    await self._notify_data_callbacks(domain, poll_result.data)
                
            except asyncio.CancelledError:
                logger.info(f"Polling cancelled for domain: {domain}")
                break
            except Exception as e:
                logger.error(f"Error in polling loop for {domain}: {e}")
                await asyncio.sleep(5)  # Brief pause before retry
    
    async def _poll_domain(self, domain: str) -> PollResult:
        """Poll a specific domain using appropriate endpoints"""
        state = self.domain_states[domain]
        
        if domain not in self.endpoints or not self.endpoints[domain]:
            return PollResult(
                success=False,
                error_message=f"No endpoints available for domain: {domain}"
            )
        
        available_endpoints = self.endpoints[domain]
        
        # Try endpoints in order of priority
        for attempt, endpoint in enumerate(available_endpoints):
            if attempt < state.active_source_index:
                continue  # Skip lower priority sources unless we're falling back
            
            try:
                result = await self._poll_endpoint(endpoint, domain)
                
                if result.success:
                    # Reset to highest priority source on success
                    if state.active_source_index != 0:
                        logger.info(f"Restored primary source for {domain}")
                        state.active_source_index = 0
                    
                    state.consecutive_failures = 0
                    return result
                else:
                    logger.warning(f"Failed to poll {endpoint.source_name} for {domain}: {result.error_message}")
                    
            except Exception as e:
                logger.error(f"Exception polling {endpoint.source_name} for {domain}: {e}")
        
        # All endpoints failed - increment failure count and try fallback
        state.consecutive_failures += 1
        
        if state.consecutive_failures >= 3 and state.active_source_index < len(available_endpoints) - 1:
            # Switch to fallback source
            state.active_source_index += 1
            logger.warning(f"Switching to fallback source {state.active_source_index} for {domain}")
        
        # Return cached data if available
        if state.cached_data:
            logger.info(f"Using cached data for {domain} due to polling failures")
            return PollResult(
                success=True,
                data=state.cached_data,
                cached=True,
                source_name="cache"
            )
        
        return PollResult(
            success=False,
            error_message=f"All sources failed for domain: {domain}"
        )
    
    async def _poll_endpoint(self, endpoint: DiscoveredEndpoint, domain: str) -> PollResult:
        """Poll a specific endpoint"""
        start_time = time.time()
        
        # Check rate limits
        if not self._check_rate_limit(endpoint.source_name):
            return PollResult(
                success=False,
                error_message="Rate limit exceeded",
                source_name=endpoint.source_name
            )
        
        # Handle file:// URLs
        if endpoint.url.startswith('file://'):
            return await self._poll_file_endpoint(endpoint, domain)
        
        # Build URL with parameters if needed
        poll_url = self._build_poll_url(endpoint, domain)
        
        # Check cache first
        cache_key = f"poll:{endpoint.source_name}:{domain}:{poll_url}"
        cached_entry = self.cache.get(cache_key)
        
        headers = {}
        
        if cached_entry:
            # Use ETags/Last-Modified for efficient polling
            if cached_entry.etag:
                headers['If-None-Match'] = cached_entry.etag
            if cached_entry.last_modified:
                headers['If-Modified-Since'] = cached_entry.last_modified
        
        try:
            async with self.session.get(poll_url, headers=headers) as response:
                duration_ms = int((time.time() - start_time) * 1000)
                
                if response.status == 304:
                    # Not modified - return cached data
                    return PollResult(
                        success=True,
                        data=cached_entry.data if cached_entry else None,
                        source_name=endpoint.source_name,
                        endpoint_url=poll_url,
                        http_status=304,
                        cached=True,
                        poll_duration_ms=duration_ms
                    )
                
                if response.status != 200:
                    return PollResult(
                        success=False,
                        source_name=endpoint.source_name,
                        endpoint_url=poll_url,
                        http_status=response.status,
                        error_message=f"HTTP {response.status}",
                        poll_duration_ms=duration_ms
                    )
                
                # Parse response
                try:
                    raw_data = await response.json()
                except json.JSONDecodeError as e:
                    return PollResult(
                        success=False,
                        source_name=endpoint.source_name,
                        endpoint_url=poll_url,
                        http_status=response.status,
                        error_message=f"Invalid JSON: {e}",
                        poll_duration_ms=duration_ms
                    )
                
                # Parse and normalize data
                normalized_data = self.parser.parse_data(raw_data, domain, endpoint.source_name)
                
                # Cache the result
                etag = response.headers.get('ETag')
                last_modified = response.headers.get('Last-Modified')
                
                self.cache.set(
                    cache_key,
                    normalized_data,
                    ttl_seconds=self._get_cache_ttl(domain),
                    etag=etag,
                    last_modified=last_modified,
                    source_url=poll_url
                )
                
                return PollResult(
                    success=True,
                    data=normalized_data,
                    source_name=endpoint.source_name,
                    endpoint_url=poll_url,
                    http_status=response.status,
                    poll_duration_ms=duration_ms,
                    etag=etag,
                    last_modified=last_modified
                )
        
        except asyncio.TimeoutError:
            return PollResult(
                success=False,
                source_name=endpoint.source_name,
                endpoint_url=poll_url,
                error_message="Request timeout",
                poll_duration_ms=int((time.time() - start_time) * 1000)
            )
        except Exception as e:
            return PollResult(
                success=False,
                source_name=endpoint.source_name,
                endpoint_url=poll_url,
                error_message=str(e),
                poll_duration_ms=int((time.time() - start_time) * 1000)
            )
    
    async def _poll_file_endpoint(self, endpoint: DiscoveredEndpoint, domain: str) -> PollResult:
        """Poll a local file endpoint"""
        start_time = time.time()
        
        try:
            file_path = endpoint.url.replace('file://', '')
            
            with open(file_path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
            
            # Parse and normalize data
            normalized_data = self.parser.parse_data(raw_data, domain, endpoint.source_name)
            
            return PollResult(
                success=True,
                data=normalized_data,
                source_name=endpoint.source_name,
                endpoint_url=endpoint.url,
                poll_duration_ms=int((time.time() - start_time) * 1000)
            )
            
        except Exception as e:
            return PollResult(
                success=False,
                source_name=endpoint.source_name,
                endpoint_url=endpoint.url,
                error_message=str(e),
                poll_duration_ms=int((time.time() - start_time) * 1000)
            )
    
    def _build_poll_url(self, endpoint: DiscoveredEndpoint, domain: str) -> str:
        """Build poll URL with current parameters"""
        url = endpoint.url
        
        if endpoint.requires_params and endpoint.sample_params:
            # Update parameters with current values
            params = endpoint.sample_params.copy()
            
            # Update date parameters
            if 'date' in params:
                params['date'] = time.strftime('%Y-%m-%d')
            
            # Update season parameters
            if 'season' in params:
                current_year = time.gmtime().tm_year
                if time.gmtime().tm_mon <= 6:  # Before July = previous season
                    season_start = current_year - 1
                else:
                    season_start = current_year
                params['season'] = f"{season_start}{season_start + 1}"
            
            try:
                url = url.format(**params)
            except KeyError as e:
                logger.warning(f"Missing parameter for URL template: {e}")
        
        return url
    
    def _check_rate_limit(self, source_name: str) -> bool:
        """Check if we're within rate limits for a source"""
        current_time = time.time()
        
        if source_name not in self.rate_limits:
            self.rate_limits[source_name] = []
        
        # Clean old entries (older than 1 minute)
        self.rate_limits[source_name] = [
            t for t in self.rate_limits[source_name] 
            if current_time - t < 60
        ]
        
        # Check if we're under the limit
        source_config = None
        for domain_sources in self.config.data_sources.values():
            for source in domain_sources.values():
                if source.name == source_name:
                    source_config = source
                    break
        
        if source_config:
            limit = source_config.rate_limit_per_minute
            if len(self.rate_limits[source_name]) >= limit:
                return False
        
        # Add current request
        self.rate_limits[source_name].append(current_time)
        return True
    
    def _get_cache_ttl(self, domain: str) -> int:
        """Get appropriate cache TTL for domain"""
        ttl_map = {
            'live_game': 30,    # 30 seconds for live games
            'standings': 3600,  # 1 hour for standings
            'team_info': 86400, # 24 hours for team info
            'schedule': 3600,   # 1 hour for schedule
            'player_stats': 1800 # 30 minutes for player stats
        }
        
        return ttl_map.get(domain, 3600)
    
    async def _update_domain_state(self, domain: str, poll_result: PollResult):
        """Update domain state based on poll result"""
        state = self.domain_states[domain]
        current_time = time.time()
        
        state.last_poll_time = current_time
        
        if poll_result.success:
            state.last_successful_poll = current_time
            if poll_result.data and not poll_result.cached:
                state.cached_data = poll_result.data
                
                # Extract game state for adaptive intervals
                if domain == 'live_game':
                    self._update_game_state(state, poll_result.data)
        
        # Calculate next interval
        state.current_interval = self._calculate_adaptive_interval(domain, state, poll_result)
        
        logger.debug(f"Domain {domain}: next poll in {state.current_interval}s")
    
    def _update_game_state(self, state: DomainState, data: Dict[str, Any]):
        """Update game state information for adaptive polling"""
        games = data.get('games', [])
        
        if not games:
            state.is_live_game = False
            state.game_state = "NO_GAMES"
            return
        
        # Check for live games
        live_games = [g for g in games if g.get('gameState') in ['LIVE', 'CRIT']]
        
        if live_games:
            state.is_live_game = True
            state.game_state = "LIVE"
            
            # Check for intermission
            game = live_games[0]
            clock_info = game.get('clock', {})
            state.intermission = clock_info.get('inIntermission', False)
        else:
            state.is_live_game = False
            
            # Check for upcoming games today
            upcoming_games = [g for g in games if g.get('gameState') in ['FUT', 'PRE']]
            if upcoming_games:
                state.game_state = "SCHEDULED"
            else:
                final_games = [g for g in games if g.get('gameState') in ['FINAL', 'OFF']]
                if final_games:
                    state.game_state = "FINAL"
                else:
                    state.game_state = "OFF_DAY"
    
    def _calculate_adaptive_interval(self, domain: str, state: DomainState, 
                                   poll_result: PollResult) -> int:
        """Calculate adaptive polling interval based on domain and current state"""
        polling_config = self.config.polling
        
        if domain == 'live_game':
            if state.is_live_game:
                if state.intermission:
                    return polling_config.live_game_slow
                else:
                    return polling_config.live_game_fast
            elif state.game_state == "SCHEDULED":
                return polling_config.pregame
            elif state.game_state == "FINAL":
                return polling_config.postgame
            else:
                return polling_config.offline
        
        elif domain == 'standings':
            return polling_config.standings
        
        elif domain == 'team_info':
            return max(polling_config.standings, 3600)  # At least 1 hour
        
        elif domain == 'schedule':
            return polling_config.schedule
        
        elif domain == 'player_stats':
            return polling_config.player_stats
        
        # Default fallback
        return 300
    
    async def _notify_data_callbacks(self, domain: str, data: Dict[str, Any]):
        """Notify registered callbacks of new data"""
        if domain in self.data_callbacks:
            for callback in self.data_callbacks[domain]:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(domain, data)
                    else:
                        callback(domain, data)
                except Exception as e:
                    logger.error(f"Error in data callback for {domain}: {e}")
    
    def register_data_callback(self, domain: str, callback: Callable):
        """Register a callback for data updates"""
        if domain not in self.data_callbacks:
            self.data_callbacks[domain] = []
        self.data_callbacks[domain].append(callback)
    
    def get_latest_data(self, domain: str) -> Optional[Dict[str, Any]]:
        """Get latest cached data for a domain"""
        if domain in self.domain_states:
            return self.domain_states[domain].cached_data
        return None
    
    def get_polling_stats(self) -> Dict[str, Any]:
        """Get comprehensive polling statistics"""
        stats = {
            'polling_active': self.polling_active,
            'total_domains': len(self.domain_states),
            'domains': {}
        }
        
        for domain, state in self.domain_states.items():
            stats['domains'][domain] = {
                'last_poll_time': state.last_poll_time,
                'last_successful_poll': state.last_successful_poll,
                'current_interval': state.current_interval,
                'consecutive_failures': state.consecutive_failures,
                'active_source_index': state.active_source_index,
                'game_state': state.game_state,
                'is_live_game': state.is_live_game,
                'intermission': state.intermission,
                'has_cached_data': state.cached_data is not None
            }
        
        return stats


def create_poller(config: Backend2Config, cache_manager: CacheManager, 
                 endpoints: Dict[str, List[DiscoveredEndpoint]], parser: DataParser) -> AdaptivePoller:
    """Create and initialize adaptive poller"""
    return AdaptivePoller(config, cache_manager, endpoints, parser)
