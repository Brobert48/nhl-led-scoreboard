"""
Data parser and normalizer for Backend v2

Handles:
- Mapping raw JSON from any source to the normalized scoreboard schema
- Using cached schema fingerprints and key-path mapping
- Fallback parsing strategies when schema changes are detected
- Maintaining compatibility with existing renderer expectations
"""

import json
import logging
import re
from typing import Dict, List, Any, Optional, Union, Callable
from datetime import datetime, timezone
from dataclasses import dataclass
from copy import deepcopy

from .config import Backend2Config
from .cache import CacheManager, SchemaFingerprint

logger = logging.getLogger(__name__)


@dataclass
class ParsedGameData:
    """Normalized game data structure matching renderer expectations"""
    games: List[Dict[str, Any]]
    gameDate: str
    source_info: Dict[str, str]


@dataclass
class ParsingRule:
    """Rule for mapping source data to target schema"""
    source_path: str
    target_path: str
    transform: Optional[Callable] = None
    required: bool = True
    default_value: Any = None


class DataParser:
    """Main data parser that normalizes different source formats"""
    
    def __init__(self, config: Backend2Config, cache_manager: CacheManager):
        self.config = config
        self.cache = cache_manager
        
        # Initialize parsing rules for each domain and source
        self.parsing_rules = self._initialize_parsing_rules()
        
        # Team ID/name mappings for normalization
        self.team_mappings = self._load_team_mappings()
    
    def _initialize_parsing_rules(self) -> Dict[str, Dict[str, List[ParsingRule]]]:
        """Initialize parsing rules for different sources and domains"""
        rules = {
            'live_game': {
                'nhl_official': self._get_nhl_official_game_rules(),
                'espn': self._get_espn_game_rules(),
                'nhl_legacy': self._get_nhl_legacy_game_rules()
            },
            'standings': {
                'nhl_official': self._get_nhl_official_standings_rules(),
                'espn': self._get_espn_standings_rules()
            },
            'team_info': {
                'nhl_official': self._get_nhl_official_team_rules(),
                'backup_json': self._get_backup_team_rules()
            },
            'schedule': {
                'nhl_official': self._get_nhl_official_schedule_rules(),
                'espn': self._get_espn_schedule_rules()
            },
            'player_stats': {
                'nhl_official': self._get_nhl_official_player_rules()
            },
            'playoffs': {
                'nhl_official': self._get_nhl_official_playoffs_rules(),
                'espn': self._get_espn_playoffs_rules()
            },
            'season_schedule': {
                'nhl_official': self._get_nhl_official_season_rules(),
                'espn': self._get_espn_season_rules()
            }
        }
        
        return rules
    
    def _get_nhl_official_game_rules(self) -> List[ParsingRule]:
        """Parsing rules for NHL official API game data"""
        return [
            # Top-level game array
            ParsingRule('games', 'games', required=True),
            ParsingRule('date', 'gameDate', required=False),
            
            # Individual game mapping (applied to each game in array)
            ParsingRule('id', 'id', required=True),
            ParsingRule('gameDate', 'gameDate', required=True),
            ParsingRule('startTimeUTC', 'startTimeUTC', required=True),
            ParsingRule('gameState', 'gameState', required=True),
            ParsingRule('gameType', 'gameType', required=False, default_value=2),
            
            # Away team
            ParsingRule('awayTeam.id', 'awayTeam.id', required=True),
            ParsingRule('awayTeam.name.default', 'awayTeam.name.default', required=True),
            ParsingRule('awayTeam.abbrev', 'awayTeam.abbrev', required=True),
            ParsingRule('awayTeam.score', 'awayTeam.score', required=False, default_value=0),
            ParsingRule('awayTeam.sog', 'awayTeam.sog', required=False, default_value=0),
            
            # Home team
            ParsingRule('homeTeam.id', 'homeTeam.id', required=True),
            ParsingRule('homeTeam.name.default', 'homeTeam.name.default', required=True),
            ParsingRule('homeTeam.abbrev', 'homeTeam.abbrev', required=True),
            ParsingRule('homeTeam.score', 'homeTeam.score', required=False, default_value=0),
            ParsingRule('homeTeam.sog', 'homeTeam.sog', required=False, default_value=0),
            
            # Clock and period info
            ParsingRule('clock.timeRemaining', 'clock.timeRemaining', required=False),
            ParsingRule('clock.inIntermission', 'clock.inIntermission', required=False, default_value=False),
            ParsingRule('periodDescriptor.number', 'periodDescriptor.number', required=False, default_value=1),
            ParsingRule('periodDescriptor.periodType', 'periodDescriptor.periodType', required=False),
            
            # Plays and situation
            ParsingRule('plays', 'plays', required=False, default_value=[]),
            ParsingRule('situation', 'situation', required=False),
            ParsingRule('rosterSpots', 'rosterSpots', required=False, default_value=[])
        ]
    
    def _get_espn_game_rules(self) -> List[ParsingRule]:
        """Parsing rules for ESPN API game data"""
        return [
            # ESPN uses different structure - map to expected format
            ParsingRule('events', 'games', transform=self._transform_espn_events),
            ParsingRule('day.date', 'gameDate', transform=self._transform_espn_date),
            
            # Individual ESPN event to game mapping
            ParsingRule('id', 'id', required=True),
            ParsingRule('date', 'gameDate', transform=self._extract_date_from_iso),
            ParsingRule('date', 'startTimeUTC', transform=self._iso_to_utc),
            ParsingRule('status.type.name', 'gameState', transform=self._map_espn_status),
            
            # ESPN team structure
            ParsingRule('competitions[0].competitors[1].team.id', 'awayTeam.id', required=True),
            ParsingRule('competitions[0].competitors[1].team.displayName', 'awayTeam.name.default', required=True),
            ParsingRule('competitions[0].competitors[1].team.abbreviation', 'awayTeam.abbrev', required=True),
            ParsingRule('competitions[0].competitors[1].score', 'awayTeam.score', transform=int, default_value=0),
            
            ParsingRule('competitions[0].competitors[0].team.id', 'homeTeam.id', required=True),
            ParsingRule('competitions[0].competitors[0].team.displayName', 'homeTeam.name.default', required=True),
            ParsingRule('competitions[0].competitors[0].team.abbreviation', 'homeTeam.abbrev', required=True),
            ParsingRule('competitions[0].competitors[0].score', 'homeTeam.score', transform=int, default_value=0),
        ]
    
    def _get_nhl_legacy_game_rules(self) -> List[ParsingRule]:
        """Parsing rules for NHL legacy API"""
        # Similar to official but with some structural differences
        return self._get_nhl_official_game_rules()
    
    def _get_nhl_official_standings_rules(self) -> List[ParsingRule]:
        """Parsing rules for NHL official standings"""
        return [
            ParsingRule('standings', 'standings', required=True),
            
            # Individual standing entry
            ParsingRule('teamName.default', 'teamName.default', required=True),
            ParsingRule('teamAbbrev.default', 'teamAbbrev.default', required=True),
            ParsingRule('wins', 'wins', required=True),
            ParsingRule('losses', 'losses', required=True),
            ParsingRule('otLosses', 'otLosses', required=False, default_value=0),
            ParsingRule('points', 'points', required=True),
            ParsingRule('divisionSequence', 'divisionSequence', required=False),
            ParsingRule('conferenceSequence', 'conferenceSequence', required=False),
            ParsingRule('leagueSequence', 'leagueSequence', required=False)
        ]
    
    def _get_espn_standings_rules(self) -> List[ParsingRule]:
        """Parsing rules for ESPN standings"""
        return [
            ParsingRule('children[0].standings.entries', 'standings', transform=self._transform_espn_standings),
            
            # ESPN standings entry structure
            ParsingRule('team.displayName', 'teamName.default', required=True),
            ParsingRule('team.abbreviation', 'teamAbbrev.default', required=True),
            ParsingRule('stats[0].value', 'wins', transform=int, required=True),
            ParsingRule('stats[1].value', 'losses', transform=int, required=True),
            ParsingRule('stats[2].value', 'otLosses', transform=int, default_value=0),
            ParsingRule('stats[7].value', 'points', transform=int, required=True)
        ]
    
    def _get_nhl_official_team_rules(self) -> List[ParsingRule]:
        """Parsing rules for NHL official team data"""
        return [
            ParsingRule('data', 'data', required=True),
            
            # Individual team entry
            ParsingRule('id', 'id', required=True),
            ParsingRule('triCode', 'triCode', required=True),
            ParsingRule('fullName', 'fullName', required=True),
            ParsingRule('teamName', 'teamName', required=False),
            ParsingRule('locationName', 'locationName', required=False)
        ]
    
    def _get_backup_team_rules(self) -> List[ParsingRule]:
        """Parsing rules for backup team JSON"""
        return self._get_nhl_official_team_rules()  # Same structure
    
    def _get_nhl_official_schedule_rules(self) -> List[ParsingRule]:
        """Parsing rules for NHL official schedule"""
        return [
            ParsingRule('games', 'games', required=True),
            
            # Individual game in schedule
            ParsingRule('id', 'id', required=True),
            ParsingRule('gameDate', 'gameDate', required=True),
            ParsingRule('startTimeUTC', 'startTimeUTC', required=True),
            ParsingRule('gameState', 'gameState', required=True),
            ParsingRule('awayTeam', 'awayTeam', required=True),
            ParsingRule('homeTeam', 'homeTeam', required=True)
        ]
    
    def _get_espn_schedule_rules(self) -> List[ParsingRule]:
        """Parsing rules for ESPN schedule"""
        return [
            ParsingRule('events', 'games', transform=self._transform_espn_events),
            
            # Same individual mapping as ESPN games
            ParsingRule('id', 'id', required=True),
            ParsingRule('date', 'gameDate', transform=self._extract_date_from_iso),
            ParsingRule('date', 'startTimeUTC', transform=self._iso_to_utc),
            ParsingRule('status.type.name', 'gameState', transform=self._map_espn_status)
        ]
    
    def _get_nhl_official_player_rules(self) -> List[ParsingRule]:
        """Parsing rules for NHL official player stats"""
        return [
            ParsingRule('people', 'people', required=True),
            
            # Individual player
            ParsingRule('id', 'id', required=True),
            ParsingRule('fullName', 'fullName', required=True),
            ParsingRule('currentTeam.id', 'currentTeam.id', required=False),
            ParsingRule('stats', 'stats', required=False, default_value=[])
        ]
    
    def _get_nhl_official_playoffs_rules(self) -> List[ParsingRule]:
        """Parsing rules for NHL official playoffs data"""
        return [
            ParsingRule('rounds', 'rounds', required=True),
            ParsingRule('defaultRound', 'defaultRound', required=True),
            ParsingRule('season', 'season', required=False),
            
            # Individual series mapping
            ParsingRule('seriesLetter', 'seriesLetter', required=True),
            ParsingRule('matchupTeams', 'matchupTeams', required=True),
            ParsingRule('currentGame.seriesStatus', 'currentGame.seriesStatus', required=False),
            ParsingRule('seriesRecord.wins', 'seriesRecord.wins', required=False, default_value=0),
            ParsingRule('seriesRecord.losses', 'seriesRecord.losses', required=False, default_value=0)
        ]
    
    def _get_espn_playoffs_rules(self) -> List[ParsingRule]:
        """Parsing rules for ESPN playoffs data"""
        return [
            ParsingRule('tournaments', 'rounds', transform=self._transform_espn_tournaments),
            ParsingRule('season.year', 'season', required=False),
            
            # ESPN tournament structure
            ParsingRule('name', 'seriesLetter', required=False),
            ParsingRule('competitors', 'matchupTeams', transform=self._transform_espn_competitors),
        ]
    
    def _get_nhl_official_season_rules(self) -> List[ParsingRule]:
        """Parsing rules for NHL season schedule data"""
        return [
            ParsingRule('seasonId', 'seasonId', required=True),
            ParsingRule('regularSeasonStartDate', 'regularSeasonStartDate', required=False),
            ParsingRule('regularSeasonEndDate', 'regularSeasonEndDate', required=False),
            ParsingRule('playoffStartDate', 'playoffStartDate', required=False),
            ParsingRule('playoffEndDate', 'playoffEndDate', required=False),
            ParsingRule('startDate', 'startDate', required=False),
            ParsingRule('endDate', 'endDate', required=False)
        ]
    
    def _get_espn_season_rules(self) -> List[ParsingRule]:
        """Parsing rules for ESPN season data"""
        return [
            ParsingRule('season.year', 'seasonId', transform=self._transform_espn_season_id),
            ParsingRule('season.startDate', 'regularSeasonStartDate', transform=self._extract_date_from_iso),
            ParsingRule('season.endDate', 'regularSeasonEndDate', transform=self._extract_date_from_iso)
        ]
    
    def parse_data(self, raw_data: Dict[str, Any], domain: str, source_name: str) -> Dict[str, Any]:
        """Parse raw data from any source into normalized format"""
        try:
            # Get parsing rules for this domain and source
            rules = self._get_parsing_rules(domain, source_name)
            if not rules:
                logger.warning(f"No parsing rules found for {source_name}/{domain}")
                return raw_data
            
            # Apply adaptive parsing with schema fingerprint validation
            normalized_data = self._apply_adaptive_parsing(raw_data, rules, domain, source_name)
            
            # Add metadata
            normalized_data['_source_info'] = {
                'source_name': source_name,
                'domain': domain,
                'parsed_at': datetime.now(timezone.utc).isoformat(),
                'original_structure_hash': self._calculate_structure_hash(raw_data)
            }
            
            return normalized_data
            
        except Exception as e:
            logger.error(f"Failed to parse data from {source_name}/{domain}: {e}")
            # Return raw data as fallback
            return raw_data
    
    def _get_parsing_rules(self, domain: str, source_name: str) -> Optional[List[ParsingRule]]:
        """Get parsing rules for specific domain and source"""
        if domain not in self.parsing_rules:
            return None
        
        domain_rules = self.parsing_rules[domain]
        
        # Try exact source name match first
        if source_name in domain_rules:
            return domain_rules[source_name]
        
        # Try fuzzy matching for source names
        for rule_source_name in domain_rules.keys():
            if source_name.lower() in rule_source_name.lower() or rule_source_name.lower() in source_name.lower():
                return domain_rules[rule_source_name]
        
        return None
    
    def _apply_adaptive_parsing(self, raw_data: Dict[str, Any], rules: List[ParsingRule], 
                              domain: str, source_name: str) -> Dict[str, Any]:
        """Apply parsing rules with adaptive schema handling"""
        
        # Check if we have a cached schema fingerprint
        cached_fingerprint = self.cache.get_schema_fingerprint(source_name, domain)
        
        if cached_fingerprint:
            # Check for schema drift
            current_fingerprint, changes = self.cache.update_schema_fingerprint(raw_data, source_name, domain)
            
            if changes and changes['structural_change_score'] > 0.1:
                logger.warning(f"Schema drift detected for {source_name}/{domain}: {changes}")
                # Attempt adaptive rule discovery
                rules = self._adapt_rules_for_schema_changes(rules, changes, raw_data)
        
        # Apply the parsing rules
        return self._apply_rules(raw_data, rules)
    
    def _adapt_rules_for_schema_changes(self, original_rules: List[ParsingRule], 
                                      changes: Dict[str, Any], raw_data: Dict[str, Any]) -> List[ParsingRule]:
        """Adapt parsing rules when schema changes are detected"""
        adapted_rules = original_rules.copy()
        
        # Handle removed keys - mark as optional or find alternatives
        for removed_key in changes.get('removed_keys', []):
            # Find rules that reference this key
            for rule in adapted_rules:
                if removed_key in rule.source_path:
                    logger.info(f"Adapting rule for removed key: {removed_key}")
                    # Try to find similar key path
                    alternative_key = self._find_alternative_key_path(removed_key, raw_data)
                    if alternative_key:
                        rule.source_path = rule.source_path.replace(removed_key, alternative_key)
                        logger.info(f"Mapped {removed_key} to {alternative_key}")
                    else:
                        # Mark as optional with default
                        rule.required = False
                        if rule.default_value is None:
                            rule.default_value = self._get_default_for_target_path(rule.target_path)
        
        # Handle new keys - potentially useful for fallback
        for new_key in changes.get('new_keys', []):
            logger.debug(f"New key available: {new_key}")
            # Could be used for enhanced data extraction in future
        
        return adapted_rules
    
    def _find_alternative_key_path(self, removed_key: str, data: Dict[str, Any]) -> Optional[str]:
        """Find alternative key path when original is removed"""
        # Extract the key name (last part after dots/brackets)
        key_name = removed_key.split('.')[-1].split('[')[0]
        
        # Search for similar keys in the data
        all_paths = self._extract_all_key_paths(data)
        
        # Look for exact name matches first
        for path in all_paths:
            if path.endswith(key_name) and path != removed_key:
                return path
        
        # Look for partial matches
        for path in all_paths:
            if key_name.lower() in path.lower() and path != removed_key:
                return path
        
        return None
    
    def _extract_all_key_paths(self, data: Any, path: str = "") -> List[str]:
        """Extract all key paths from nested data structure"""
        paths = []
        
        if isinstance(data, dict):
            for key, value in data.items():
                current_path = f"{path}.{key}" if path else key
                paths.append(current_path)
                paths.extend(self._extract_all_key_paths(value, current_path))
        elif isinstance(data, list) and data:
            if path:
                paths.append(f"{path}[0]")
            paths.extend(self._extract_all_key_paths(data[0], f"{path}[0]" if path else "[0]"))
        
        return paths
    
    def _apply_rules(self, data: Dict[str, Any], rules: List[ParsingRule]) -> Dict[str, Any]:
        """Apply parsing rules to transform data"""
        result = {}
        
        # Handle array data (games, standings, etc.)
        if self._is_array_data(data, rules):
            return self._apply_rules_to_array(data, rules)
        
        # Apply rules to single object
        for rule in rules:
            try:
                value = self._extract_value_by_path(data, rule.source_path)
                
                if value is None:
                    if rule.required:
                        logger.warning(f"Required field missing: {rule.source_path}")
                    value = rule.default_value
                
                if value is not None and rule.transform:
                    value = rule.transform(value)
                
                if value is not None:
                    self._set_value_by_path(result, rule.target_path, value)
                    
            except Exception as e:
                logger.debug(f"Failed to apply rule {rule.source_path} -> {rule.target_path}: {e}")
                if rule.required:
                    logger.warning(f"Failed to parse required field: {rule.source_path}")
        
        return result
    
    def _is_array_data(self, data: Dict[str, Any], rules: List[ParsingRule]) -> bool:
        """Check if this data contains arrays that need individual processing"""
        # Look for array indicators in rules
        array_indicators = ['games', 'standings', 'events', 'people', 'data']
        
        for rule in rules:
            if any(indicator in rule.source_path for indicator in array_indicators):
                source_value = self._extract_value_by_path(data, rule.source_path)
                if isinstance(source_value, list):
                    return True
        
        return False
    
    def _apply_rules_to_array(self, data: Dict[str, Any], rules: List[ParsingRule]) -> Dict[str, Any]:
        """Apply rules to data containing arrays"""
        result = {}
        
        # Find the main array field
        array_field = None
        array_data = None
        
        for rule in rules:
            if rule.source_path in ['games', 'standings', 'events', 'people', 'data']:
                array_data = self._extract_value_by_path(data, rule.source_path)
                if isinstance(array_data, list):
                    array_field = rule.target_path
                    break
        
        if not array_data:
            return self._apply_rules(data, rules)
        
        # Apply non-array rules first (top-level fields)
        for rule in rules:
            if not any(array_indicator in rule.source_path for array_indicator in ['games', 'standings', 'events', 'people', 'data']):
                try:
                    value = self._extract_value_by_path(data, rule.source_path)
                    if value is not None and rule.transform:
                        value = rule.transform(value)
                    if value is not None:
                        self._set_value_by_path(result, rule.target_path, value)
                except Exception:
                    pass
        
        # Process array items
        processed_array = []
        item_rules = [rule for rule in rules if not rule.source_path in ['games', 'standings', 'events', 'people', 'data']]
        
        for item in array_data:
            processed_item = self._apply_rules(item, item_rules)
            if processed_item:
                processed_array.append(processed_item)
        
        if array_field:
            self._set_value_by_path(result, array_field, processed_array)
        
        return result
    
    def _extract_value_by_path(self, data: Any, path: str) -> Any:
        """Extract value from nested data using dot notation path"""
        if not path or data is None:
            return data
        
        current = data
        parts = path.split('.')
        
        for part in parts:
            if current is None:
                return None
            
            # Handle array indexing
            if '[' in part and ']' in part:
                key = part.split('[')[0]
                index_str = part.split('[')[1].split(']')[0]
                
                if key and key in current:
                    current = current[key]
                
                if isinstance(current, list):
                    try:
                        index = int(index_str)
                        if 0 <= index < len(current):
                            current = current[index]
                        else:
                            return None
                    except ValueError:
                        return None
                else:
                    return None
            else:
                # Regular key access
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    return None
        
        return current
    
    def _set_value_by_path(self, data: Dict[str, Any], path: str, value: Any):
        """Set value in nested data using dot notation path"""
        if not path:
            return
        
        parts = path.split('.')
        current = data
        
        # Navigate to parent
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        
        # Set final value
        final_key = parts[-1]
        current[final_key] = value
    
    def _get_default_for_target_path(self, target_path: str) -> Any:
        """Get appropriate default value based on target path"""
        if 'score' in target_path.lower():
            return 0
        elif 'sog' in target_path.lower():
            return 0
        elif 'intermission' in target_path.lower():
            return False
        elif 'plays' in target_path.lower():
            return []
        elif 'id' in target_path.lower():
            return 0
        elif 'name' in target_path.lower():
            return "Unknown"
        elif 'state' in target_path.lower():
            return "UNKNOWN"
        else:
            return None
    
    def _calculate_structure_hash(self, data: Any) -> str:
        """Calculate hash of data structure for change detection"""
        import hashlib
        
        def extract_structure(obj):
            if isinstance(obj, dict):
                return {k: extract_structure(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [type(item).__name__ for item in obj[:3]]  # Sample first 3 items
            else:
                return type(obj).__name__
        
        structure = extract_structure(data)
        structure_str = json.dumps(structure, sort_keys=True)
        return hashlib.md5(structure_str.encode()).hexdigest()
    
    def _load_team_mappings(self) -> Dict[str, Any]:
        """Load team ID/name mappings for normalization"""
        # Could load from cache or config
        # For now, return empty dict - will be populated as needed
        return {}
    
    # Transform functions for specific source formats
    
    def _transform_espn_events(self, events: List[Dict]) -> List[Dict]:
        """Transform ESPN events to games format"""
        # ESPN returns events, we need to rename to games
        return events
    
    def _transform_espn_date(self, date_str: str) -> str:
        """Transform ESPN date format"""
        # ESPN may use different date format
        return date_str
    
    def _extract_date_from_iso(self, iso_string: str) -> str:
        """Extract date part from ISO datetime string"""
        try:
            return iso_string.split('T')[0]
        except:
            return iso_string
    
    def _iso_to_utc(self, iso_string: str) -> str:
        """Convert ISO string to UTC format expected by renderer"""
        try:
            # Parse and reformat to expected UTC format
            dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        except:
            return iso_string
    
    def _map_espn_status(self, espn_status: str) -> str:
        """Map ESPN status to NHL API status"""
        status_mapping = {
            'STATUS_SCHEDULED': 'FUT',
            'STATUS_IN_PROGRESS': 'LIVE', 
            'STATUS_FINAL': 'FINAL',
            'STATUS_POSTPONED': 'PPD',
            'STATUS_CANCELED': 'CAN'
        }
        
        return status_mapping.get(espn_status, espn_status)
    
    def _transform_espn_standings(self, entries: List[Dict]) -> List[Dict]:
        """Transform ESPN standings entries"""
        # ESPN has different structure, may need more complex transformation
        return entries
    
    def _transform_espn_tournaments(self, tournaments: List[Dict]) -> Dict[str, Any]:
        """Transform ESPN tournaments to NHL rounds format"""
        if not tournaments:
            return {}
        
        # Convert ESPN tournament structure to NHL rounds structure
        rounds = {}
        for i, tournament in enumerate(tournaments):
            rounds[str(i + 1)] = {
                'series': tournament.get('events', []),
                'name': tournament.get('name', f'Round {i + 1}')
            }
        
        return rounds
    
    def _transform_espn_competitors(self, competitors: List[Dict]) -> List[Dict]:
        """Transform ESPN competitors to NHL matchupTeams format"""
        if not competitors or len(competitors) < 2:
            return []
        
        matchup_teams = []
        for competitor in competitors:
            team_data = {
                'team': {
                    'id': competitor.get('team', {}).get('id'),
                    'name': competitor.get('team', {}).get('displayName'),
                    'triCode': competitor.get('team', {}).get('abbreviation')
                },
                'seriesRecord': {
                    'wins': competitor.get('wins', 0),
                    'losses': competitor.get('losses', 0)
                }
            }
            matchup_teams.append(team_data)
        
        return matchup_teams
    
    def _transform_espn_season_id(self, year: int) -> str:
        """Transform ESPN season year to NHL season ID format"""
        if isinstance(year, int):
            return f"{year}{year + 1}"
        return str(year)


def create_parser(config: Backend2Config, cache_manager: CacheManager) -> DataParser:
    """Create and initialize data parser"""
    return DataParser(config, cache_manager)
