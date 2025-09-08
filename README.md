
# NHL-LED-scoreboard 

![scoreboard demo](assets/images/scoreboard.jpg)

# NHL LED Scoreboard Raspberry Pi Image
[![Create Release - Image](https://github.com/falkyre/nhl-led-scoreboard-img/actions/workflows/main.yml/badge.svg)](https://github.com/falkyre/nhl-led-scoreboard-img/actions/workflows/main.yml)
[![GitHub release (latest by date)](https://badgen.net/github/release/falkyre/nhl-led-scoreboard-img?label=Version)](https://github.com/falkyre/nhl-led-scoreboard-img/releases/latest)

[![discord button](assets/images/discord_button.png)](https://discord.gg/CWa5CzK)

# Backend v2 - Robust Multi-Source Data System

## 🚀 NEW: Backend v2 Available

Due to ongoing NHL API changes and reliability issues, we've developed **Backend v2** - a robust, multi-source data acquisition system that automatically discovers and validates data sources with intelligent fallback capabilities.

### Key Features

- **🔄 Multi-Source Support**: Automatically discovers NHL Official API, ESPN, and other data sources
- **🛡️ Intelligent Fallback**: Switches to backup sources when primary feeds fail  
- **⚡ Adaptive Polling**: Adjusts refresh rates based on game state (3s during play, 20s intermission, etc.)
- **🔍 Schema Detection**: Automatically adapts to API changes using schema fingerprinting
- **🥧 Pi-Optimized**: Designed specifically for Raspberry Pi resource constraints
- **💾 Smart Caching**: Efficient caching with ETags and Last-Modified headers
- **📊 Zero Renderer Changes**: Drop-in replacement - existing display code unchanged

### Quick Start with Backend v2

1. **Install Additional Dependencies**:
   ```bash
   pip install aiohttp pyyaml beautifulsoup4
   ```

2. **Create Configuration**:
   ```bash
   python -m src.backend2.main --create-config
   ```

3. **Edit Configuration**:
   ```bash
   nano config/backend2.yaml
   ```
   Update `preferred_teams` and `timezone` for your location.

4. **Test Discovery**:
   ```bash
   python -m src.backend2.main --test-discovery
   ```

5. **Run Backend v2**:
   ```bash
   python -m src.backend2.main
   ```

6. **Use with Existing Renderer** (replace `src/main.py` data initialization):
   ```python
   # Replace this line in src/main.py:
   # data = Data(config)
   
   # With Backend v2:
   from src.backend2.main import Backend2Main
   backend = Backend2Main("config/backend2.yaml")
   await backend.initialize()
   await backend.start()
   data = backend.get_data_interface()
   ```

### Configuration Options

The `config/backend2.yaml` file provides extensive customization:

```yaml
preferred_teams:
  - "Toronto Maple Leafs"
  - "Boston Bruins"

timezone: "America/New_York"

polling:
  live_game_fast: 3      # Seconds during active play
  live_game_slow: 20     # Seconds during intermissions
  pregame: 60           # Seconds before game start
  
data_sources:
  live_game:
    nhl_official:
      base_url: "https://api-web.nhle.com/v1"
      priority: 1
    espn:
      base_url: "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl"
      priority: 2
```

### Monitoring Backend v2

Check system status:
```bash
python -m src.backend2.main --status
```

Monitor logs for source switching and schema changes:
```bash
tail -f /var/log/nhl-scoreboard-backend2.log
```

## Legacy System (Original - Deprecated)

## (2024-05-17) Previous API Issues
After NHL API changes, the original system became unreliable. Backend v2 addresses these issues with automatic source discovery and fallback capabilities. 

## Shout-out

First, these two for making this repo top notch and already working on future versions:

- [Josh Kay](https://github.com/joshkay)

- [Sean Ostermann](https://github.com/falkyre)

This project was inspired by the [mlb-led-scoreboard](https://github.com/MLB-LED-Scoreboard/mlb-led-scoreboard). Go check it out and try it on your board, even if you are not a baseball fan, it's amazing.

I also used this [nhlscoreboard repo](https://github.com/quarterturn/nhlscoreboard) as a guide at the very beginning as I was learning python.
You all can thank [Drew Hynes](https://gitlab.com/dword4) for his hard work on documenting the free [nhl api](https://gitlab.com/dword4/nhlapi).

## Licensing

This project uses the GNU Public License. If you intend to sell these, the code must remain open source.
