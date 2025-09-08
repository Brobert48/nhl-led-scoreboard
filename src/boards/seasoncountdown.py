from rgbmatrix import graphics
from PIL import ImageFont, Image
from utils import center_text
from datetime import datetime, date
import debug
from time import sleep
from utils import get_file

PATH = 'assets/logos'
LOGO_LINK = "https://www-league.nhlstatic.com/images/logos/league-dark/133-flat.svg"

class SeasonCountdown:
    def __init__(self, data, matrix,sleepEvent):
        
        self.data = data
        self.matrix = matrix
        self.sleepEvent = sleepEvent
        self.sleepEvent.clear()
        self.font = data.config.layout.font
        self.font.large = data.config.layout.font_large_2
        
        # Get countdown data from Backend v2
        countdown_data = getattr(data, 'season_countdown', {})
        
        if countdown_data:
            # Use Backend v2 data
            self.days_until_season = countdown_data.get('days_until_season', 0)
            self.season_started = countdown_data.get('season_started', False)
            self.nextseason = countdown_data.get('season_year', 'Unknown')
            self.nextseason_short = f"NHL {self.nextseason}"
            
            # Get actual season start date if available
            season_start_date = countdown_data.get('season_start_date')
            if isinstance(season_start_date, str):
                try:
                    self.season_start = datetime.strptime(season_start_date, '%Y-%m-%d').date()
                except ValueError:
                    self.season_start = date.today()
            else:
                self.season_start = season_start_date or date.today()
        else:
            # Fallback for when Backend v2 data isn't available
            debug.warning("No Backend v2 countdown data available, using fallback calculation")
            current_year = date.today().year
            next_year = current_year + 1
            
            # Estimate season start as early October
            if date.today().month >= 10:
                self.season_start = date(current_year + 1, 10, 7)
            else:
                self.season_start = date(current_year, 10, 7)
            
            self.days_until_season = (self.season_start - date.today()).days
            self.season_started = self.days_until_season <= 0
            self.nextseason = f"{current_year}-{str(next_year)[-2:]}"
            self.nextseason_short = f"NHL {self.nextseason}"
        
        self.scroll_pos = self.matrix.width

    def draw(self):
        
        debug.info("NHL Countdown Launched")

        #for testing purposes
        #self.days_until_season = 0

        debug.info(str(self.days_until_season) + " days to NHL Season")

        if self.season_started or self.days_until_season <= 0:
            self.season_start_today()
        else:
            self.season_countdown()
  
    
    def season_start_today(self) :
        #  it's just like Christmas!
        self.matrix.clear()

        nhl_logo = Image.open(get_file('assets/logos/_local/nhl_logo_64x32.png'))

        self.matrix.draw_image((15,0), nhl_logo)
        
        debug.info("{0} season has begun".format(self.nextseason))

        self.matrix.render()
        self.sleepEvent.wait(0.5)


        #draw bottom text        
        self.matrix.draw_text(
            (14,25), 
            self.nextseason, 
            font=self.font,
            fill=(0,0,0),
            backgroundColor=(155,155,155)
        )
        self.matrix.render()
        self.sleepEvent.wait(15)

    def season_countdown(self) :
        
        self.matrix.clear()

        nhl_logo = Image.open(get_file('assets/logos/_local/nhl_logo_64x32.png'))
        black_gradiant = Image.open(get_file('assets/images/64x32_scoreboard_center_gradient.png'))

        self.matrix.draw_image((34,0), nhl_logo)
        self.matrix.draw_image((-5,0), black_gradiant)
        
        debug.info("Counting down to {0}".format(self.nextseason_short))

        self.matrix.render()
        self.sleepEvent.wait(0.5)

        #draw days to xmas
        self.matrix.draw_text(
            (1,2),
            str(self.days_until_season),
             font=self.font.large,
             fill=(255,165,0)
        )
        
        self.matrix.render()
        self.sleepEvent.wait(1)

        #draw bottom text        
        self.matrix.draw_text(
            (1,18), 
            "DAYS til", 
            font=self.font,
            fill=(255,165,0)
        )

        self.matrix.render()
        self.sleepEvent.wait(1)

        #draw bottom text        
        self.matrix.draw_text(
            (1,25), 
            self.nextseason_short, 
            font=self.font,
            fill=(0,0,0),
            backgroundColor=(155,155,155)
        )

        self.matrix.render()
        self.sleepEvent.wait(15)