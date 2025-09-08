"""
Lightweight caching system for Backend v2

Handles caching of:
- API responses and ETags
- Schema fingerprints for drift detection
- Source URLs and their validation status
- Last known good data for fallback scenarios
"""

import json
import sqlite3
import os
import hashlib
import time
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta

from .config import CacheConfig

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """Represents a cached data entry"""
    key: str
    data: Any
    timestamp: float
    ttl_seconds: int
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    source_url: Optional[str] = None
    content_hash: Optional[str] = None


@dataclass
class SchemaFingerprint:
    """Schema fingerprint for detecting API changes"""
    source_name: str
    domain: str
    key_paths: List[str]
    type_signatures: Dict[str, str]
    sample_values: Dict[str, Any]
    timestamp: float
    version_hash: str


class JSONCache:
    """Simple JSON-based cache for Pi compatibility"""
    
    def __init__(self, cache_dir: str):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.index_file = self.cache_dir / "cache_index.json"
        self._load_index()
    
    def _load_index(self):
        """Load cache index from disk"""
        try:
            if self.index_file.exists():
                with open(self.index_file, 'r', encoding='utf-8') as f:
                    self.index = json.load(f)
            else:
                self.index = {}
        except Exception as e:
            logger.warning(f"Failed to load cache index: {e}")
            self.index = {}
    
    def _save_index(self):
        """Save cache index to disk"""
        try:
            with open(self.index_file, 'w', encoding='utf-8') as f:
                json.dump(self.index, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save cache index: {e}")
    
    def _get_cache_path(self, key: str) -> Path:
        """Get file path for cache key"""
        # Use hash to avoid filesystem issues with special characters
        key_hash = hashlib.md5(key.encode()).hexdigest()
        return self.cache_dir / f"{key_hash}.json"
    
    def get(self, key: str) -> Optional[CacheEntry]:
        """Get cached entry by key"""
        if key not in self.index:
            return None
        
        entry_info = self.index[key]
        cache_path = self._get_cache_path(key)
        
        if not cache_path.exists():
            # Clean up stale index entry
            del self.index[key]
            self._save_index()
            return None
        
        # Check TTL
        if time.time() > entry_info['expires_at']:
            self.delete(key)
            return None
        
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            return CacheEntry(
                key=key,
                data=data['data'],
                timestamp=data['timestamp'],
                ttl_seconds=data['ttl_seconds'],
                etag=data.get('etag'),
                last_modified=data.get('last_modified'),
                source_url=data.get('source_url'),
                content_hash=data.get('content_hash')
            )
        except Exception as e:
            logger.warning(f"Failed to load cache entry {key}: {e}")
            self.delete(key)
            return None
    
    def set(self, key: str, data: Any, ttl_seconds: int = 3600,
            etag: Optional[str] = None, last_modified: Optional[str] = None,
            source_url: Optional[str] = None) -> bool:
        """Set cache entry"""
        try:
            cache_path = self._get_cache_path(key)
            timestamp = time.time()
            
            # Calculate content hash for change detection
            content_hash = hashlib.sha256(
                json.dumps(data, sort_keys=True).encode()
            ).hexdigest()
            
            cache_data = {
                'data': data,
                'timestamp': timestamp,
                'ttl_seconds': ttl_seconds,
                'etag': etag,
                'last_modified': last_modified,
                'source_url': source_url,
                'content_hash': content_hash
            }
            
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2)
            
            # Update index
            self.index[key] = {
                'expires_at': timestamp + ttl_seconds,
                'content_hash': content_hash,
                'cached_at': timestamp
            }
            self._save_index()
            
            return True
        except Exception as e:
            logger.error(f"Failed to cache entry {key}: {e}")
            return False
    
    def delete(self, key: str) -> bool:
        """Delete cache entry"""
        try:
            cache_path = self._get_cache_path(key)
            if cache_path.exists():
                cache_path.unlink()
            
            if key in self.index:
                del self.index[key]
                self._save_index()
            
            return True
        except Exception as e:
            logger.error(f"Failed to delete cache entry {key}: {e}")
            return False
    
    def cleanup_expired(self):
        """Remove expired cache entries"""
        current_time = time.time()
        expired_keys = []
        
        for key, info in self.index.items():
            if current_time > info['expires_at']:
                expired_keys.append(key)
        
        for key in expired_keys:
            self.delete(key)
        
        logger.info(f"Cleaned up {len(expired_keys)} expired cache entries")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        total_entries = len(self.index)
        total_size = 0
        expired_count = 0
        current_time = time.time()
        
        for info in self.index.values():
            cache_path = self._get_cache_path(list(self.index.keys())[0])  # Sample path for size calc
            if cache_path.exists():
                total_size += cache_path.stat().st_size
            
            if current_time > info['expires_at']:
                expired_count += 1
        
        return {
            'total_entries': total_entries,
            'expired_entries': expired_count,
            'total_size_bytes': total_size,
            'cache_directory': str(self.cache_dir)
        }


class SchemaCache:
    """Cache for API schema fingerprints"""
    
    def __init__(self, cache_dir: str):
        self.cache_dir = Path(cache_dir) / "schemas"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_schema_path(self, source_name: str, domain: str) -> Path:
        """Get file path for schema fingerprint"""
        filename = f"{source_name}_{domain}.json"
        return self.cache_dir / filename
    
    def get_fingerprint(self, source_name: str, domain: str) -> Optional[SchemaFingerprint]:
        """Get cached schema fingerprint"""
        schema_path = self._get_schema_path(source_name, domain)
        
        if not schema_path.exists():
            return None
        
        try:
            with open(schema_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            return SchemaFingerprint(**data)
        except Exception as e:
            logger.warning(f"Failed to load schema fingerprint: {e}")
            return None
    
    def set_fingerprint(self, fingerprint: SchemaFingerprint) -> bool:
        """Save schema fingerprint"""
        try:
            schema_path = self._get_schema_path(fingerprint.source_name, fingerprint.domain)
            
            with open(schema_path, 'w', encoding='utf-8') as f:
                json.dump(asdict(fingerprint), f, indent=2)
            
            return True
        except Exception as e:
            logger.error(f"Failed to save schema fingerprint: {e}")
            return False
    
    def compare_fingerprints(self, old: SchemaFingerprint, new: SchemaFingerprint) -> Dict[str, Any]:
        """Compare two schema fingerprints and return differences"""
        changes = {
            'version_changed': old.version_hash != new.version_hash,
            'new_keys': [],
            'removed_keys': [],
            'type_changes': {},
            'structural_change_score': 0.0
        }
        
        old_keys = set(old.key_paths)
        new_keys = set(new.key_paths)
        
        changes['new_keys'] = list(new_keys - old_keys)
        changes['removed_keys'] = list(old_keys - new_keys)
        
        # Check for type changes in common keys
        for key in old_keys.intersection(new_keys):
            old_type = old.type_signatures.get(key)
            new_type = new.type_signatures.get(key)
            if old_type != new_type:
                changes['type_changes'][key] = {
                    'old_type': old_type,
                    'new_type': new_type
                }
        
        # Calculate structural change score (0.0 = no change, 1.0 = complete change)
        total_keys = len(old_keys.union(new_keys))
        if total_keys > 0:
            changed_keys = len(changes['new_keys']) + len(changes['removed_keys']) + len(changes['type_changes'])
            changes['structural_change_score'] = changed_keys / total_keys
        
        return changes


def create_schema_fingerprint(data: Dict[str, Any], source_name: str, domain: str) -> SchemaFingerprint:
    """Create a schema fingerprint from JSON data"""
    key_paths = []
    type_signatures = {}
    sample_values = {}
    
    def extract_paths(obj: Any, path: str = ""):
        """Recursively extract key paths and types"""
        if isinstance(obj, dict):
            for key, value in obj.items():
                current_path = f"{path}.{key}" if path else key
                key_paths.append(current_path)
                type_signatures[current_path] = type(value).__name__
                
                # Store sample values for primitive types
                if isinstance(value, (str, int, float, bool)) and len(str(value)) < 100:
                    sample_values[current_path] = value
                
                extract_paths(value, current_path)
        elif isinstance(obj, list) and obj:
            # Sample the first item in arrays
            extract_paths(obj[0], f"{path}[0]")
    
    extract_paths(data)
    
    # Create version hash from structure
    structure_str = json.dumps({
        'paths': sorted(key_paths),
        'types': {k: v for k, v in sorted(type_signatures.items())}
    }, sort_keys=True)
    
    version_hash = hashlib.sha256(structure_str.encode()).hexdigest()
    
    return SchemaFingerprint(
        source_name=source_name,
        domain=domain,
        key_paths=key_paths,
        type_signatures=type_signatures,
        sample_values=sample_values,
        timestamp=time.time(),
        version_hash=version_hash
    )


class CacheManager:
    """Main cache manager that coordinates JSON and schema caches"""
    
    def __init__(self, config: CacheConfig):
        self.config = config
        self.json_cache = JSONCache(config.base_path)
        self.schema_cache = SchemaCache(config.base_path)
        self._last_cleanup = time.time()
    
    def get(self, key: str) -> Optional[CacheEntry]:
        """Get cached data"""
        return self.json_cache.get(key)
    
    def set(self, key: str, data: Any, ttl_seconds: Optional[int] = None, **kwargs) -> bool:
        """Set cached data"""
        ttl = ttl_seconds or self.config.max_age_seconds
        return self.json_cache.set(key, data, ttl, **kwargs)
    
    def get_schema_fingerprint(self, source_name: str, domain: str) -> Optional[SchemaFingerprint]:
        """Get cached schema fingerprint"""
        return self.schema_cache.get_fingerprint(source_name, domain)
    
    def update_schema_fingerprint(self, data: Dict[str, Any], source_name: str, domain: str) -> Tuple[SchemaFingerprint, Optional[Dict[str, Any]]]:
        """Update schema fingerprint and return changes if any"""
        new_fingerprint = create_schema_fingerprint(data, source_name, domain)
        old_fingerprint = self.get_schema_fingerprint(source_name, domain)
        
        changes = None
        if old_fingerprint:
            changes = self.schema_cache.compare_fingerprints(old_fingerprint, new_fingerprint)
            if changes['structural_change_score'] > 0.1:  # 10% change threshold
                logger.warning(f"Significant schema changes detected for {source_name}/{domain}: {changes}")
        
        self.schema_cache.set_fingerprint(new_fingerprint)
        return new_fingerprint, changes
    
    def cleanup_if_needed(self):
        """Cleanup expired entries if enough time has passed"""
        current_time = time.time()
        if current_time - self._last_cleanup > self.config.cleanup_interval:
            self.json_cache.cleanup_expired()
            self._last_cleanup = current_time
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics"""
        return {
            'json_cache': self.json_cache.get_stats(),
            'schema_cache_dir': str(self.schema_cache.cache_dir),
            'last_cleanup': self._last_cleanup,
            'config': asdict(self.config)
        }
