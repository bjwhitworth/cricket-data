#!/usr/bin/env python3
"""
Unit tests for generate_match_narrative.py
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
import sys
import os

# Add scripts/python to path so we can import the module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts', 'python'))
from generate_match_narrative import _row_to_dict, format_match_prompt


class TestRowToDict:
    """Test the _row_to_dict helper function."""
    
    def test_converts_row_to_dict(self):
        """Should convert a database row tuple to a dictionary."""
        cursor = Mock()
        cursor.description = [('col1',), ('col2',), ('col3',)]
        row = ('value1', 'value2', 'value3')
        
        result = _row_to_dict(cursor, row)
        
        assert result == {'col1': 'value1', 'col2': 'value2', 'col3': 'value3'}
    
    def test_handles_none_row(self):
        """Should return None when row is None."""
        cursor = Mock()
        cursor.description = [('col1',)]
        
        result = _row_to_dict(cursor, None)
        
        assert result is None
    
    def test_handles_empty_row(self):
        """Should handle empty tuples."""
        cursor = Mock()
        cursor.description = []
        row = ()
        
        result = _row_to_dict(cursor, row)
        
        assert result == {}


class TestFormatMatchPrompt:
    """Test the format_match_prompt function."""
    
    @pytest.fixture
    def sample_match_data(self):
        """Sample match data for testing."""
        return {
            'match_info': {
                'match_id': '1234567',
                'event_name': 'Test Series 2025',
                'city': 'Melbourne',
                'venue': 'MCG',
                'match_start_date': '2025-01-01',
                'team_1': 'Australia',
                'team_2': 'England',
                'toss_winner': 'Australia',
                'toss_decision': 'bat',
                'winner': 'Australia',
                'result_type': 'runs',
                'result_description': '50 runs',
                'winner_after_eliminator': None,
                'outcome_method': None,
                'players_of_match': 'S Smith'
            },
            'innings': [
                {
                    'innings_number': 1,
                    'batting_team': 'Australia',
                    'is_super_over': False,
                    'runs_total': 350,
                    'wickets_fallen': 8,
                    'recorded_over_count': 50
                },
                {
                    'innings_number': 2,
                    'batting_team': 'England',
                    'is_super_over': False,
                    'runs_total': 300,
                    'wickets_fallen': 10,
                    'recorded_over_count': 48
                }
            ],
            'top_batters': [
                {
                    'innings_number': 1,
                    'batter': 'S Smith',
                    'runs': 120,
                    'balls_faced': 100,
                    'fours': 10,
                    'sixes': 2
                }
            ],
            'top_bowlers': [
                {
                    'innings_number': 2,
                    'bowler': 'P Cummins',
                    'balls_bowled': 60,
                    'runs_conceded': 45,
                    'wickets': 4
                }
            ],
            'key_wickets': [
                {
                    'innings_number': 2,
                    'over_number': 10,
                    'ball_in_over': 3,
                    'wicket_player_out': 'J Root',
                    'wicket_kind': 'caught',
                    'bowler': 'P Cummins',
                    'wicket_fielder_1': 'D Warner',
                    'wicket_fielder_2': None
                }
            ]
        }
    
    def test_prompt_includes_match_details(self, sample_match_data):
        """Should include all match metadata in prompt."""
        prompt = format_match_prompt(sample_match_data)
        
        assert 'Test Series 2025' in prompt
        assert 'MCG, Melbourne' in prompt
        assert 'Australia vs England' in prompt
        assert 'Australia won and chose to bat' in prompt
        assert 'S Smith' in prompt
    
    def test_prompt_includes_innings_summaries(self, sample_match_data):
        """Should include innings summaries."""
        prompt = format_match_prompt(sample_match_data)
        
        assert 'Innings 1' in prompt
        assert 'Australia scored 350/8 in 50 overs' in prompt
        assert 'Innings 2' in prompt
        assert 'England scored 300/10 in 48 overs' in prompt
    
    def test_prompt_includes_super_over_flag(self, sample_match_data):
        """Should flag super over innings."""
        sample_match_data['innings'][0]['is_super_over'] = True
        
        prompt = format_match_prompt(sample_match_data)
        
        assert '(Super Over)' in prompt
    
    def test_prompt_includes_batting_performances(self, sample_match_data):
        """Should include batter stats with strike rate."""
        prompt = format_match_prompt(sample_match_data)
        
        assert 'S Smith - 120 runs' in prompt
        assert '(100 balls' in prompt
        assert '10 fours' in prompt
        assert '2 sixes' in prompt
        assert 'SR: 120.0' in prompt
    
    def test_prompt_includes_bowling_performances(self, sample_match_data):
        """Should include bowler stats with overs."""
        prompt = format_match_prompt(sample_match_data)
        
        assert 'P Cummins - 4/45' in prompt
        assert '(10.0 overs)' in prompt
    
    def test_prompt_includes_wickets(self, sample_match_data):
        """Should include wicket details with fielders."""
        prompt = format_match_prompt(sample_match_data)
        
        assert 'Over 10.3' in prompt
        assert 'J Root caught b P Cummins (c D Warner)' in prompt
    
    def test_handles_wicket_without_fielders(self, sample_match_data):
        """Should handle wickets without fielders (e.g., bowled)."""
        sample_match_data['key_wickets'][0]['wicket_fielder_1'] = None
        sample_match_data['key_wickets'][0]['wicket_kind'] = 'bowled'
        
        prompt = format_match_prompt(sample_match_data)
        
        assert 'J Root bowled b P Cummins' in prompt
        assert '(c ' not in prompt.split('J Root')[1].split('\n')[0]
    
    def test_handles_two_fielders(self, sample_match_data):
        """Should handle wickets with two fielders (run out)."""
        sample_match_data['key_wickets'][0]['wicket_fielder_2'] = 'S Smith'
        
        prompt = format_match_prompt(sample_match_data)
        
        assert '(c D Warner & S Smith)' in prompt
    
    def test_handles_zero_balls_faced(self, sample_match_data):
        """Should handle zero division for strike rate."""
        sample_match_data['top_batters'][0]['balls_faced'] = 0
        
        prompt = format_match_prompt(sample_match_data)
        
        assert 'SR: 0.0' in prompt
    
    def test_prompt_limits_batters(self, sample_match_data):
        """Should limit top batters to 6."""
        sample_match_data['top_batters'] = [
            {'innings_number': i, 'batter': f'Player{i}', 'runs': 50, 
             'balls_faced': 40, 'fours': 5, 'sixes': 1}
            for i in range(10)
        ]
        
        prompt = format_match_prompt(sample_match_data)
        
        # Count how many batters appear
        batter_count = sum(1 for i in range(10) if f'Player{i}' in prompt)
        assert batter_count == 6
    
    def test_prompt_limits_bowlers(self, sample_match_data):
        """Should limit top bowlers to 6."""
        sample_match_data['top_bowlers'] = [
            {'innings_number': i, 'bowler': f'Bowler{i}', 'balls_bowled': 60,
             'runs_conceded': 40, 'wickets': 3}
            for i in range(10)
        ]
        
        prompt = format_match_prompt(sample_match_data)
        
        # Count how many bowlers appear
        bowler_count = sum(1 for i in range(10) if f'Bowler{i}' in prompt)
        assert bowler_count == 6
    
    def test_prompt_limits_wickets(self, sample_match_data):
        """Should limit key wickets to 8."""
        sample_match_data['key_wickets'] = [
            {
                'innings_number': 1,
                'over_number': i,
                'ball_in_over': 1,
                'wicket_player_out': f'Player{i}',
                'wicket_kind': 'caught',
                'bowler': 'Bowler',
                'wicket_fielder_1': None,
                'wicket_fielder_2': None
            }
            for i in range(12)
        ]
        
        prompt = format_match_prompt(sample_match_data)
        
        # Count how many players appear in wickets section
        player_count = sum(1 for i in range(12) if f'Player{i}' in prompt)
        assert player_count == 8
    
    def test_handles_tie_no_result(self, sample_match_data):
        """Should handle matches with no winner."""
        sample_match_data['match_info']['winner'] = None
        sample_match_data['match_info']['winner_after_eliminator'] = None
        
        prompt = format_match_prompt(sample_match_data)
        
        assert 'Tie/No Result' in prompt


class TestFetchMatchData:
    """Test the fetch_match_data function (requires mocking)."""
    
    @patch('generate_match_narrative.duckdb.connect')
    def test_raises_error_for_nonexistent_match(self, mock_connect):
        """Should raise ValueError if match not found."""
        from generate_match_narrative import fetch_match_data
        
        mock_conn = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None
        mock_cursor.description = []
        mock_conn.execute.return_value = mock_cursor
        
        with pytest.raises(ValueError, match="Match 999999 not found"):
            fetch_match_data('999999')


class TestGenerateNarrative:
    """Test the generate_narrative function (requires mocking)."""
    
    @patch('generate_match_narrative.fetch_match_data')
    @patch('generate_match_narrative.genai.Client')
    @patch.dict(os.environ, {'GEMINI_API_KEY': 'test-key'})
    def test_generates_narrative(self, mock_client_class, mock_fetch):
        """Should generate narrative from match data."""
        from generate_match_narrative import generate_narrative
        
        # Mock data
        mock_fetch.return_value = {
            'match_info': {
                'event_name': 'Test', 'city': 'City', 'venue': 'Venue',
                'match_start_date': '2025-01-01', 'team_1': 'A', 'team_2': 'B',
                'toss_winner': 'A', 'toss_decision': 'bat', 'winner': 'A',
                'result_type': 'runs', 'result_description': '50 runs',
                'winner_after_eliminator': None, 'outcome_method': None,
                'players_of_match': 'Player'
            },
            'innings': [],
            'top_batters': [],
            'top_bowlers': [],
            'key_wickets': []
        }
        
        # Mock API response
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_response = Mock()
        mock_response.text = "This is the generated narrative."
        mock_client.models.generate_content.return_value = mock_response
        
        result = generate_narrative('1234567')
        
        assert result == "This is the generated narrative."
        mock_client.models.generate_content.assert_called_once()
    
    @patch('generate_match_narrative.fetch_match_data')
    def test_raises_error_without_api_key(self, mock_fetch):
        """Should raise ValueError if API key not set."""
        from generate_match_narrative import generate_narrative
        
        mock_fetch.return_value = {
            'match_info': {
                'event_name': 'Test', 'city': 'City', 'venue': 'Venue',
                'match_start_date': '2025-01-01', 'team_1': 'A', 'team_2': 'B',
                'toss_winner': 'A', 'toss_decision': 'bat', 'winner': 'A',
                'result_type': 'runs', 'result_description': '50 runs',
                'winner_after_eliminator': None, 'outcome_method': None,
                'players_of_match': 'Player'
            },
            'innings': [],
            'top_batters': [],
            'top_bowlers': [],
            'key_wickets': []
        }
        
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="GEMINI_API_KEY"):
                generate_narrative('1234567')


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
