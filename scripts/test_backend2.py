#!/usr/bin/env python3
"""
Test script for Backend v2

Quick validation that all components can be imported and initialized
without requiring actual network connectivity.
"""

import sys
import os
import asyncio
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

async def test_backend2():
    """Test Backend v2 components"""
    print("Testing NHL LED Scoreboard Backend v2...")
    
    try:
        # Test imports
        print("✓ Testing imports...")
        from backend2.config import Backend2Config, load_config
        from backend2.cache import CacheManager
        from backend2.discovery import DataSourceDiscovery
        from backend2.parser import DataParser
        from backend2.poller import AdaptivePoller
        from backend2.renderer_adapter import RendererAdapter
        from backend2.main import Backend2Main
        print("✓ All imports successful")
        
        # Test config loading
        print("✓ Testing configuration...")
        config = Backend2Config()  # Default config
        print(f"✓ Default config loaded with {len(config.preferred_teams)} preferred teams")
        
        # Test cache manager
        print("✓ Testing cache manager...")
        cache_manager = CacheManager(config.cache)
        print("✓ Cache manager initialized")
        
        # Test data parser
        print("✓ Testing data parser...")
        parser = DataParser(config, cache_manager)
        print("✓ Data parser initialized")
        
        # Test discovery (without actual network calls)
        print("✓ Testing discovery system...")
        discovery = DataSourceDiscovery(config, cache_manager)
        print(f"✓ Discovery initialized with {len(discovery.domain_patterns)} domain patterns")
        
        # Test sample data parsing
        print("✓ Testing data parsing...")
        sample_game_data = {
            "games": [{
                "id": 2023020001,
                "gameDate": "2023-10-01",
                "startTimeUTC": "2023-10-01T23:00:00Z",
                "gameState": "FINAL",
                "awayTeam": {
                    "id": 10,
                    "name": {"default": "Toronto Maple Leafs"},
                    "abbrev": "TOR",
                    "score": 3,
                    "sog": 25
                },
                "homeTeam": {
                    "id": 8,
                    "name": {"default": "Boston Bruins"},
                    "abbrev": "BOS", 
                    "score": 2,
                    "sog": 28
                }
            }],
            "gameDate": "2023-10-01"
        }
        
        parsed_data = parser.parse_data(sample_game_data, "live_game", "test_source")
        print(f"✓ Sample data parsed successfully with {len(parsed_data.get('games', []))} games")
        
        print("\n🎉 All Backend v2 components test successfully!")
        print("\nNext steps:")
        print("1. Create config: python -m src.backend2.main --create-config")
        print("2. Test discovery: python -m src.backend2.main --test-discovery")
        print("3. Run backend: python -m src.backend2.main")
        
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Make sure you're running from the project root directory")
        return False
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(test_backend2())
    sys.exit(0 if success else 1)
