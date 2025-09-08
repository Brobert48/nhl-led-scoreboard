"""
Main orchestrator for Backend v2

Coordinates discovery, polling, parsing, and rendering in a loop with
adaptive sleep intervals per domain. Handles startup, shutdown, and
error recovery.
"""

import asyncio
import logging
import signal
import sys
import time
from pathlib import Path
from typing import Dict, Any, Optional

from .config import Backend2Config, load_config
from .cache import CacheManager
from .discovery import DataSourceDiscovery, run_discovery
from .parser import create_parser
from .poller import create_poller
from .renderer_adapter import create_renderer_adapter

logger = logging.getLogger(__name__)


class Backend2Main:
    """Main orchestrator for Backend v2 system"""
    
    def __init__(self, config_path: str = "config/backend2.yaml"):
        self.config_path = config_path
        self.config: Optional[Backend2Config] = None
        self.cache_manager: Optional[CacheManager] = None
        self.discovery: Optional[DataSourceDiscovery] = None
        self.parser = None
        self.poller = None
        self.renderer_adapter = None
        
        # Runtime state
        self.running = False
        self.startup_complete = False
        self.endpoints = {}
        
        # Performance monitoring
        self.start_time = 0
        self.stats = {
            'startup_duration': 0,
            'discovery_duration': 0,
            'total_polls': 0,
            'successful_polls': 0,
            'cache_hits': 0,
            'cache_misses': 0
        }
    
    async def initialize(self):
        """Initialize all backend components"""
        logger.info("Initializing NHL LED Scoreboard Backend v2...")
        self.start_time = time.time()
        
        # Load configuration
        self.config = load_config(self.config_path)
        self._setup_logging()
        
        logger.info(f"Loaded configuration from {self.config_path}")
        logger.info(f"Preferred teams: {self.config.preferred_teams}")
        logger.info(f"Timezone: {self.config.timezone}")
        
        # Initialize cache manager
        self.cache_manager = CacheManager(self.config.cache)
        logger.info(f"Initialized cache at {self.config.cache.base_path}")
        
        # Initialize parser
        self.parser = create_parser(self.config, self.cache_manager)
        logger.info("Initialized data parser")
        
        # Run discovery
        discovery_start = time.time()
        self.endpoints = await self._run_discovery()
        self.stats['discovery_duration'] = time.time() - discovery_start
        
        # Initialize poller
        self.poller = create_poller(self.config, self.cache_manager, self.endpoints, self.parser)
        logger.info("Initialized adaptive poller")
        
        # Initialize renderer adapter
        self.renderer_adapter = create_renderer_adapter(self.config, self.poller)
        logger.info("Initialized renderer adapter")
        
        self.stats['startup_duration'] = time.time() - self.start_time
        self.startup_complete = True
        
        logger.info(f"Backend v2 initialization complete in {self.stats['startup_duration']:.2f}s")
    
    async def _run_discovery(self) -> Dict[str, Any]:
        """Run data source discovery"""
        logger.info("Starting data source discovery...")
        
        self.discovery = DataSourceDiscovery(self.config, self.cache_manager)
        endpoints = await asyncio.get_event_loop().run_in_executor(
            None, run_discovery, self.config, self.cache_manager
        )
        
        # Log discovery results
        total_endpoints = sum(len(eps) for eps in endpoints.values())
        logger.info(f"Discovery complete: {total_endpoints} endpoints across {len(endpoints)} domains")
        
        for domain, domain_endpoints in endpoints.items():
            if domain_endpoints:
                sources = set(ep.source_name for ep in domain_endpoints)
                logger.info(f"  {domain}: {len(domain_endpoints)} endpoints from {len(sources)} sources")
            else:
                logger.warning(f"  {domain}: No valid endpoints found")
        
        return endpoints
    
    async def start(self):
        """Start the backend system"""
        if self.running:
            logger.warning("Backend already running")
            return
        
        if not self.startup_complete:
            await self.initialize()
        
        logger.info("Starting Backend v2 polling system...")
        
        self.running = True
        
        # Start polling
        await self.poller.start_polling()
        
        logger.info("Backend v2 is now running and polling data sources")
        
        # Setup signal handlers
        if sys.platform != 'win32':
            loop = asyncio.get_event_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))
    
    async def stop(self):
        """Stop the backend system"""
        if not self.running:
            return
        
        logger.info("Stopping Backend v2...")
        
        self.running = False
        
        # Stop polling
        if self.poller:
            await self.poller.stop_polling()
        
        # Cleanup cache if needed
        if self.cache_manager:
            self.cache_manager.cleanup_if_needed()
        
        logger.info("Backend v2 stopped")
    
    async def run_forever(self):
        """Run the backend system indefinitely"""
        await self.start()
        
        try:
            # Main loop - mostly just monitoring and stats
            while self.running:
                await asyncio.sleep(60)  # Check every minute
                
                # Update statistics
                if self.poller:
                    polling_stats = self.poller.get_polling_stats()
                    
                    # Log periodic status
                    active_domains = sum(1 for d in polling_stats['domains'].values() 
                                       if d['has_cached_data'])
                    logger.info(f"Status: {active_domains}/{len(polling_stats['domains'])} domains active")
                
                # Cleanup cache periodically
                if self.cache_manager:
                    self.cache_manager.cleanup_if_needed()
                
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
        finally:
            await self.stop()
    
    def _setup_logging(self):
        """Setup logging configuration"""
        log_level = getattr(logging, self.config.log_level.upper(), logging.INFO)
        
        # Configure root logger
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Set specific logger levels
        logging.getLogger('aiohttp').setLevel(logging.WARNING)
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        
        if self.config.log_file:
            file_handler = logging.FileHandler(self.config.log_file)
            file_handler.setFormatter(
                logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            )
            logging.getLogger().addHandler(file_handler)
    
    def get_data_interface(self):
        """Get data interface for existing renderer"""
        if not self.renderer_adapter:
            raise RuntimeError("Backend not initialized")
        
        return self.renderer_adapter.data_interface
    
    def get_status(self) -> Dict[str, Any]:
        """Get comprehensive backend status"""
        status = {
            'running': self.running,
            'startup_complete': self.startup_complete,
            'config_path': self.config_path,
            'stats': self.stats.copy()
        }
        
        if self.startup_complete:
            status['uptime'] = time.time() - self.start_time
            
            if self.poller:
                status['polling'] = self.poller.get_polling_stats()
            
            if self.cache_manager:
                status['cache'] = self.cache_manager.get_cache_stats()
            
            # Endpoint summary
            status['endpoints'] = {
                domain: len(endpoints) 
                for domain, endpoints in self.endpoints.items()
            }
        
        return status
    
    def force_refresh_discovery(self):
        """Force refresh of endpoint discovery"""
        if not self.discovery:
            logger.warning("Discovery not initialized")
            return
        
        logger.info("Forcing discovery refresh...")
        
        # This would trigger a background discovery refresh
        # For now, log that it's requested
        logger.info("Discovery refresh requested (implementation pending)")


async def main():
    """Main entry point for Backend v2"""
    import argparse
    
    parser = argparse.ArgumentParser(description='NHL LED Scoreboard Backend v2')
    parser.add_argument('--config', default='config/backend2.yaml',
                       help='Configuration file path')
    parser.add_argument('--create-config', action='store_true',
                       help='Create sample configuration file and exit')
    parser.add_argument('--status', action='store_true',
                       help='Show status and exit')
    parser.add_argument('--test-discovery', action='store_true',
                       help='Test discovery and exit')
    
    args = parser.parse_args()
    
    if args.create_config:
        from .config import create_sample_config
        create_sample_config(args.config)
        print(f"Sample configuration created at {args.config}")
        return
    
    backend = Backend2Main(args.config)
    
    if args.test_discovery:
        await backend.initialize()
        print("\nDiscovery Results:")
        for domain, endpoints in backend.endpoints.items():
            print(f"\n{domain}:")
            for ep in endpoints:
                status = "✓" if ep.validation_success else "✗"
                print(f"  {status} {ep.source_name}: {ep.url}")
        return
    
    if args.status:
        try:
            await backend.initialize()
            status = backend.get_status()
            print("\nBackend v2 Status:")
            print(f"Running: {status['running']}")
            print(f"Startup Complete: {status['startup_complete']}")
            print(f"Config: {status['config_path']}")
            
            if 'endpoints' in status:
                print("\nEndpoints:")
                for domain, count in status['endpoints'].items():
                    print(f"  {domain}: {count}")
            
            return
        except Exception as e:
            print(f"Error getting status: {e}")
            return
    
    # Run the backend
    try:
        await backend.run_forever()
    except KeyboardInterrupt:
        print("\nShutdown requested")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
