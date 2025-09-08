"""
    Shows list of series summary (Table with each result of game).
"""
from time import sleep
from utils import center_obj
from data.playoffs import Series
from data.scoreboard import Scoreboard
from renderer.matrix import MatrixPixels
import debug
import nhlpy

class Seriesticker:
    def __init__(self, data, matrix, sleepEvent):
        self.data = data
        self.rotation_rate = 5
        self.matrix = matrix
        self.spacing = 3 # Number of pixel between each dot + 1
        self.sleepEvent = sleepEvent
        self.sleepEvent.clear()
        
        self.font = data.config.layout.font
        self.layout = self.data.config.config.layout.get_board_layout('scoreticker')
        self.team_colors = self.data.config.team_colors

    def render(self):
        # Get playoff data from Backend v2
        playoffs = getattr(self.data, 'playoffs', {})
        
        if not playoffs or not playoffs.get('rounds'):
            debug.info("No playoff data available for seriesticker")
            return
        
        # Get current round data
        rounds = playoffs.get('rounds', {})
        default_round = playoffs.get('defaultRound', 1)
        
        if str(default_round) not in rounds:
            debug.info(f"No data for playoff round {default_round}")
            return
        
        current_round = rounds[str(default_round)]
        self.allseries = current_round.get('series', [])
        self.index = 0
        self.num_series = len(self.allseries)
        
        if not self.allseries:
            debug.info("No series found in current playoff round")
            return

        for series in self.allseries:
            self.matrix.clear()
            banner_text = "Stanley Cup"
            color_banner_bg = (200,200,200)
            color_banner_text = (0,0,0)
            round_name = "Final"
            
            # Extract series information from Backend v2 structure
            series_letter = series.get('seriesLetter', '')
            matchup_teams = series.get('matchupTeams', [])
            
            # Determine round name and banner
            if default_round < 4:
                # Not the final round
                round_name = current_round.get('name', f"Round {default_round}")
                
                # Try to determine conference from series data
                if len(matchup_teams) >= 2:
                    try:
                        # Use first team's conference if available
                        first_team = matchup_teams[0]
                        team_id = first_team.get('team', {}).get('id')
                        if team_id:
                            color_conf = self.team_colors.color(f"{team_id}.primary")
                            color_banner_bg = (color_conf['r'], color_conf['g'], color_conf['b'])
                            banner_text = series_letter or "Conference"
                    except:
                        # Fallback to default colors
                        banner_text = series_letter or f"Round {default_round}"
                        
                self.show_indicator(self.index, self.num_series)
            
            self.matrix.draw_text(
                (1, 7),
                round_name,
                font=self.font,
                fill=(255,255,255)
            )
            # Conference banner, Round Title
            self.matrix.draw.rectangle([0,0,self.matrix.width,5], fill=color_banner_bg)
            self.matrix.draw_text(
                (1, 1), 
                banner_text, 
                font=self.font, 
                fill=(0,0,0)
            )
            self.index += 1
            
            self.draw_series_table(series)
            self.matrix.render()
            self.sleepEvent.wait(self.data.config.seriesticker_rotation_rate)

    def draw_series_table(self, series):
        # Extract teams from Backend v2 structure
        matchup_teams = series.get('matchupTeams', [])
        
        if len(matchup_teams) < 2:
            debug.warning("Not enough teams in series matchup")
            return False
        
        # Get team data from Backend v2 structure
        top_team = matchup_teams[0]
        bottom_team = matchup_teams[1]
        
        top_team_id = top_team.get('team', {}).get('id', 0)
        bottom_team_id = bottom_team.get('team', {}).get('id', 0)
        
        try:
            color_top_bg = self.team_colors.color("{}.primary".format(top_team_id))
            color_top_team = self.team_colors.color("{}.text".format(top_team_id))
        except:
            color_top_bg = {'r': 100, 'g': 100, 'b': 100}
            color_top_team = {'r': 255, 'g': 255, 'b': 255}

        try:
            color_bottom_bg = self.team_colors.color("{}.primary".format(bottom_team_id))
            color_bottom_team = self.team_colors.color("{}.text".format(bottom_team_id))
        except:
            color_bottom_bg = {'r': 150, 'g': 150, 'b': 150}
            color_bottom_team = {'r': 255, 'g': 255, 'b': 255}

        # Table
        self.matrix.draw.line([(0,21),(self.matrix.width,21)], width=1, fill=(150,150,150))

        # use rectangle because I want to keep symmetry for the background of team's abbrev
        self.matrix.draw.rectangle([0,14,12,20], fill=(color_top_bg['r'], color_top_bg['g'], color_top_bg['b']))
        # Get team abbreviations from Backend v2 structure
        top_team_abbrev = top_team.get('team', {}).get('triCode', 'UNK')
        bottom_team_abbrev = bottom_team.get('team', {}).get('triCode', 'UNK')
        
        self.matrix.draw_text(
            (1, 15), 
            top_team_abbrev, 
            font=self.font, 
            fill=(color_top_team['r'], color_top_team['g'], color_top_team['b'])
        )

        self.matrix.draw.rectangle([0,22,12,28], fill=(color_bottom_bg['r'], color_bottom_bg['g'], color_bottom_bg['b']))
        self.matrix.draw_text(
            (1, 23), 
            bottom_team_abbrev, 
            font=self.font, 
            fill=(color_bottom_team['r'], color_bottom_team['g'], color_bottom_team['b'])
        )
        
        rec_width = 0
        top_row = 15
        bottom_row = 23
        loosing_color = (150,150,150)

        # text offset for loosing score if the winning team has a score of 10 or higher and loosing team 
        # have a score lower then 10

        offset_correction = 0
        
        # Get series record from Backend v2 structure  
        top_team_wins = top_team.get('seriesRecord', {}).get('wins', 0)
        bottom_team_wins = bottom_team.get('seriesRecord', {}).get('wins', 0)
        
        # For Backend v2, we'll display wins instead of individual games
        # since the individual game data structure is different
        
        # Display series record as wins/losses
        max_wins = max(top_team_wins, bottom_team_wins)
        total_games = top_team_wins + bottom_team_wins
        
        # Simplified display for Backend v2 - show wins as filled rectangles
        for game_num in range(1, 8):  # Max 7 games in a series
            x_pos = 13 + (game_num - 1) * 7
            
            # Determine game result
            if game_num <= top_team_wins:
                # Top team won this game
                game_color_top = (color_top_team['r'], color_top_team['g'], color_top_team['b'])
                game_color_bottom = loosing_color
            elif game_num <= bottom_team_wins + top_team_wins:
                # Bottom team won this game (adjust for previous top team wins)
                game_color_top = loosing_color
                game_color_bottom = (color_bottom_team['r'], color_bottom_team['g'], color_bottom_team['b'])
            else:
                # Game not played yet
                game_color_top = loosing_color
                game_color_bottom = loosing_color
                
            # Draw game result indicators
            if game_num <= total_games:
                self.matrix.draw.rectangle([x_pos, top_row, x_pos + 4, top_row + 4], 
                                         fill=game_color_top)
                self.matrix.draw.rectangle([x_pos, bottom_row, x_pos + 4, bottom_row + 4], 
                                         fill=game_color_bottom)
        
        # Display series record as text
        self.matrix.draw_text(
            (50, 15),
            f"{top_team_wins}",
            font=self.font,
            fill=(255, 255, 255)
        )
        self.matrix.draw_text(
            (50, 23),
            f"{bottom_team_wins}",
            font=self.font,
            fill=(255, 255, 255)
        )
        
        return True

    def show_indicator(self, index, num_elements):
        """Show indicator dots for current series"""
        i = index + 1
        for x in range(i):
            offset = self.matrix.width - (2*x) - 2
            spacing = self.spacing - (x*2)
            # Blue indicator for current/completed series
            self.matrix.draw.ellipse([(offset - spacing,29),(offset + 2 - spacing, 31)], fill=(0,  255, 255))
        
        y = num_elements - i
        for x in range(y):
            offset = self.matrix.width - (2*x) - 2 - self.spacing
            spacing = self.spacing - (x*2)
            # Grey indicator for remaining series
            self.matrix.draw.ellipse([(offset - spacing,29),(offset + 2 - spacing, 31)], fill=(100, 100, 100))

        return True