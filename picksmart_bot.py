import discord
import pandas as pd
import requests
import json
import numpy as np 
import math

def fetch_prizepicks_data():
    response = requests.get("https://partner-api.prizepicks.com/projections?per_page=1000")
    data = response.json()
    
    projections = data["data"]
    included = data["included"]
    
    flattened_data = []
    for item in projections:
        attributes = item["attributes"]
        relationships = item["relationships"]
        league_id = relationships["league"]["data"]["id"]
        player_id = relationships["new_player"]["data"]["id"]
        projection_type_id = relationships["projection_type"]["data"]["id"]
        stat_type_id = relationships["stat_type"]["data"]["id"]
        entry_type = attributes.get("odds_type", "standard") 
        flattened_data.append({
            "id": item["id"],
            "board_time": attributes["board_time"],
            "description": attributes["description"],
            "line_score": attributes["line_score"],
            "odds_type": attributes["odds_type"],
            "projection_type": attributes["projection_type"],
            "stat_type": attributes["stat_type"],
            "league_id": league_id,
            "player_id": player_id,
            "projection_type_id": projection_type_id,
            "stat_type_id": stat_type_id,
            "start_time": attributes["start_time"],
            "status": attributes["status"],
            "updated_at": attributes["updated_at"],
            "is_promo": attributes.get("is_promo", False),
            "flash_sale_line_score": attributes.get("flash_sale_line_score"),
            "end_time": attributes.get("end_time"),
            "refundable": attributes.get("refundable", False),
            "today": attributes.get("today", False),
            "custom_image": attributes.get("custom_image"),
            "discount_percentage": attributes.get("discount_percentage"),
            "League" : attributes.get("league"), 
            "entry_type": entry_type
        })
    
    player_data = []
    for item in included:
        if item["type"] == "new_player":
            player_data.append({
                "player_id": item["id"],
                "display_name": item["attributes"]["display_name"],
                "league" : item["attributes"]["league"]
            })    
    
    df_projections = pd.DataFrame(flattened_data)
    df_players = pd.DataFrame(player_data)
    df_merged = pd.merge(df_projections, df_players, on="player_id", how="left")
    df_merged = df_merged[['display_name', 'league', 'stat_type', 'line_score', 'entry_type', 'projection_type_id', 'start_time']]
    df_merged['line_score'] = df_merged['line_score'].astype(float)
    return df_merged

def fetch_draftkings_data():
    df_sportsbooks = pd.read_csv("/Users/rshiknis/PickSmart/player_props_data.csv")
    return df_sportsbooks

def merge_over_under(df):
    # Select only the relevant columns
    
    df_over = df[df['O/U'] == 'Over'].rename(columns={
        'Odds_DraftKings': 'over_draftkings',
        'Odds_BetMGM': 'over_betmgm',
        'Perc_BetMGM': 'perc_over_betmgm',
        'Odds_Bovada': 'over_bovada',
        'Perc_Bovada': 'perc_over_bovada',
        'Odds_Caesars': 'over_caesars',
        'Perc_Caesars': 'perc_over_caesars',
        'Odds_FanDuel': 'over_fanduel',
        'Perc_FanDuel': 'perc_over_fanduel'
    })
    df_under = df[df['O/U'] == 'Under'].rename(columns={
        'Odds_DraftKings': 'under_draftkings',
        'Odds_BetMGM': 'under_betmgm',
        'Perc_BetMGM': 'perc_under_betmgm',
        'Odds_Bovada': 'under_bovada',
        'Perc_Bovada': 'perc_under_bovada',
        'Odds_Caesars': 'under_caesars',
        'Perc_Caesars': 'perc_under_caesars',
        'Odds_FanDuel': 'under_fanduel',
        'Perc_FanDuel': 'perc_under_fanduel'
    })
    
    # Merge on common identifying columns
    df_merged = pd.merge(
        df_over, df_under,
        on=['League', 'Type','Name', 'Team', 'Opp', 'Stat', 'PrizePicks', 'Line_DraftKings', 'Line_FanDuel', 'Line_BetMGM', 'Line_Bovada', 'Line_Caesars'],
        suffixes=('_over', '_under'),
        how='inner'
    )

    
    return df_merged

def handle_user_messages(df, msg) -> str:
    message = msg.content.lower()
    if message == "discrepancy":
        player_data = df[
        (df['Type'] == 'standard') & 
        (df['PrizePicks'] < df['Line_DraftKings']) &
        (df['O/U'] == 'Over') &
        (df['Odds_DraftKings'] <= -120) 
        ]
        if player_data.empty:
            player_data = df[
            (df['Type'] == 'standard') & 
            (df['PrizePicks'] > df['Line_DraftKings']) &
            (df['O/U'] == 'Under') &
            (df['Odds_DraftKings'] <= -120) 
            ]
            if player_data.empty:
                return "No discrepancies right now, check later!"
            else:
                player_data = player_data.reset_index(drop=True)
                player_data_str = player_data.to_string(index=False, header=True).split('\n')
                formatted_str = "```\n" + "\n".join(player_data_str) + "\n```"
                return formatted_str
        else:
            second_discrep = df[
            (df['Type'] == 'standard') & 
            (df['PrizePicks'] > df['Line_DraftKings']) &
            (df['O/U'] == 'Under') &
            (df['Odds_DraftKings'] <= -120) 
            ]
            if not second_discrep.empty:
                player_data = pd.concat([player_data, second_discrep], ignore_index=True)
            player_data = player_data.reset_index(drop=True)
            player_data_str = player_data.to_string(index=False, header=True).split('\n')
            formatted_str = "```\n" + "\n".join(player_data_str) + "\n```"
            return formatted_str
    elif message == 'ev plays':
        player_data = df[
            (df['Type'] == 'standard') & 
            (((df['under_draftkings'] <= -135) | (df['over_draftkings'] <= -135)) | ((df['under_fanduel'] <= -135) | (df['over_fanduel'] <= -135)) | ((df['under_caesars'] <= -135) | (df['over_caesars'] <= -135)) | ((df['under_bovada'] <= -135) | (df['over_bovada'] <= -135)) | ((df['under_betmgm'] <= -135) | (df['over_betmgm'] <= -135)))
            & ((df['Line_DraftKings'] == df['PrizePicks']) | (df['Line_FanDuel'] == df['PrizePicks']) | (df['Line_Caesars'] == df['PrizePicks']) | (df['Line_BetMGM'] == df['PrizePicks']) | (df['Line_Bovada'] == df['PrizePicks']))
        ]
        if player_data.empty:
            return "No +EV on the board right now. Check again later"
        formatted_text = ""
        for index, row in player_data.iterrows():
            name = row['Name'].title()
            formatted_text += f":comet: **{name}**\n"
            formatted_text += f"🏅: {row['League']}\n"
            formatted_text += f"🆚Matchup: {row['Team']} vs {row['Opp']}\n"
            formatted_text += f"📈Stat: {row['Stat']}\n"
            formatted_text += f"🔮 PrizePicks: {row['PrizePicks']}\n"
            formatted_text += "### Lines\n"
            if not np.isnan(row['Line_DraftKings']):
                formatted_text += f"📚 **DraftKings ({row['Line_DraftKings']})**: *Over*: **{row['over_draftkings']}** *Under*: **{row['under_draftkings']}**\n"
            if not np.isnan(row['Line_FanDuel']):
                formatted_text += f"📚 **FanDuel ({row['Line_FanDuel']})**: *Over*: **{row['over_fanduel']}** *Under*: **{row['under_fanduel']}**\n"
            if not np.isnan(row['Line_Caesars']):
                formatted_text += f"📚 **Caesars ({row['Line_Caesars']})**: *Over*: **{row['over_caesars']}** *Under*: **{row['under_caesars']}**\n"
            if not np.isnan(row['Line_BetMGM']):
                formatted_text += f"📚 **BetMGM ({row['Line_BetMGM']})**: *Over*: **{row['over_betmgm']}** *Under*: **{row['under_betmgm']}**\n"
            if not np.isnan(row['Line_Bovada']):
                formatted_text += f"📚 **Bovada ({row['Line_Bovada']})**: *Over*: **{row['over_bovada']}** *Under*: **{row['under_bovada']}**\n"
            formatted_text += '\n'
            if index < len(player_data):
                formatted_text += "---------------------------------------------------\n\n"
        return formatted_text
    elif "odds better than" in message:
        parts = message.split()
        comparison_value = parts[-1]

        # Convert the extracted part to a float or int
        try:
            comparison_value = float(comparison_value)
        except ValueError:
            comparison_value = None  # Handle the error as needed'
        if comparison_value:
            player_data = df[(df['Line_DraftKings'] == df['PrizePicks']) & (df['Type'] == 'standard') & ((df['Odds_DraftKings'] <= comparison_value))]
            if player_data.empty:
                return "No plays better than " + str(comparison_value) + " on the board right now."
            else:
                if len(player_data) > 15:
                    player_data = player_data.head(15)
                player_data = player_data.reset_index(drop=True)
                player_data_str = player_data.to_string(index=False, header=True).split('\n')
                formatted_str = "```\n" + "\n".join(player_data_str) + "\n```"
                return formatted_str
        else:
            return "No plays better than " + str(comparison_value) + " on the board right now."
    else:
        message = str(message).strip()
        player_data = df[df['Name'].str.contains(message, na=False)]
        if player_data.empty:
            return "Player not found."
        print(player_data.columns)
        formatted_text = ""
        i = 0
        for index, row in player_data.iterrows():
            if i == 0:
                name = row['Name'].title()
                formatted_text += f":comet: **{name}**\n"
                formatted_text += f"🏅: {row['League']}\n"
                formatted_text += f"🆚Matchup: {row['Team']} vs {row['Opp']}\n\n"
            formatted_text += "---------------------------------------------------\n\n"
            formatted_text += f"📈Stat: {row['Stat']}\n"
            formatted_text += f"🔮 PrizePicks: {row['PrizePicks']}\n"
            formatted_text += "### Lines\n"
            if not np.isnan(row['Line_DraftKings']):
                formatted_text += f"📚 **DraftKings ({row['Line_DraftKings']})**: *Over*: **{row['over_draftkings']}** *Under*: **{row['under_draftkings']}**\n"
            if not np.isnan(row['Line_FanDuel']):
                formatted_text += f"📚 **FanDuel ({row['Line_FanDuel']})**: *Over*: **{row['over_fanduel']}** *Under*: **{row['under_fanduel']}**\n"
            if not np.isnan(row['Line_Caesars']):
                formatted_text += f"📚 **Caesars ({row['Line_Caesars']})**: *Over*: **{row['over_caesars']}** *Under*: **{row['under_caesars']}**\n"
            if not np.isnan(row['Line_BetMGM']):
                formatted_text += f"📚 **BetMGM ({row['Line_BetMGM']})**: *Over*: **{row['over_betmgm']}** *Under*: **{row['under_betmgm']}**\n"
            if not np.isnan(row['Line_Bovada']):
                formatted_text += f"📚 **Bovada ({row['Line_Bovada']})**: *Over*: **{row['over_bovada']}** *Under*: **{row['under_bovada']}**\n"
            formatted_text += "\n"
            i = i + 1

        return formatted_text

async def process_message(df, message):
    try:
        bot_feedback = handle_user_messages(df, message)
        if bot_feedback:
            await message.channel.send(bot_feedback)
    except Exception as error:
        print(f"Error processing message: {error}")

def runBot():
    #df_prizepicks = fetch_prizepicks_data()
    df_games = fetch_draftkings_data()
    df_games["Name"] = df_games["Name"].str.lower()
    df_games = merge_over_under(df_games)
    discord_token = #INSERT TOKEN
    intents = discord.Intents.default()
    intents.message_content = True
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        print(f'{client.user} is live')

    @client.event
    async def on_message(message):
        if message.author == client.user:
            return
        await process_message(df_games, message)

    client.run(discord_token)

if __name__ == "__main__":
    runBot()
