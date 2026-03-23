import sqlite3 as sq
import pandas as pd
# import seaborn as sns
import plotly.express as px
import matplotlib.pyplot as plt
from pathlib import Path
from PIL import Image
import numpy as np
import plotly.graph_objects as go
from my_app.analysis import get_scaled_attributes

db_path = Path(__file__).parent.parent / 'data' / 'testing.db'

#this python script is for parsing and then visualizing fight data
def elo_history_plot(fighter_id):
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        query = f'''select 
          f.name as fighter,
          o.name as opponent, 
          h.winner, 
          CASE 
            WHEN h.fighter_1 = {fighter_id} THEN h.elo_1
            WHEN h.fighter_2 = {fighter_id} THEN h.elo_2
          END AS initial_elo,

          CASE 
            WHEN h.fighter_1 = {fighter_id} THEN h.new_elo_1
            WHEN h.fighter_2 = {fighter_id} THEN h.new_elo_2
          END AS new_elo,

          CASE 
            WHEN h.fighter_1 != {fighter_id} THEN h.new_elo_1
            WHEN h.fighter_2 != {fighter_id} THEN h.new_elo_2
          END AS opponent_new_elo,

          h.date
          from elo_history h 
          join fighters f on f.fighter_id = {fighter_id} 
          join fighters o on (o.fighter_id = h.fighter_1 or o.fighter_id = h.fighter_2) and o.fighter_id != {fighter_id}
          where h.fighter_1 = {fighter_id} or h.fighter_2 = {fighter_id}
          ;
            '''
        name = cursor.execute('select name from fighters where fighter_id = ?', (fighter_id,))
        df = pd.read_sql_query(query, conn)

    df['date'] = pd.to_datetime(df['date'], format = '%b. %d, %Y')
    df = df.sort_values('date')
    #print(df)

    fig = px.line(df, x="date", y="new_elo", markers=True, title="Elo History", hover_data=['new_elo', 'date', 'opponent', 'opponent_new_elo'])
    fig.update_traces(line_color="black", marker_color="red")
    fig.update_xaxes(fixedrange=True)
    fig.update_yaxes(fixedrange=True)
    fig.update_layout(
        dragmode=False
    )
    return fig

def striking_analysis_plot(fighter_id, db):

    stats = db.execute('select true_ko_power_scaled as KO_power, str_def_scaled as defence, effective_volume_scaled as volume, sig_acc_scaled as sig_acc, SApM_scaled as sapm, ' \
    'leg_kicks_scaled as leg_kicks from aggregate_striking where fighter_id = ? ', (fighter_id,)).fetchone()

    df = pd.DataFrame(dict(
    r=[stats['KO_power'], stats['volume'], stats['defence'], stats['sig_acc'], stats['leg_kicks'], stats['sapm']],
    theta=['KO power','Volume','Defense', 'Significant Accuracy', 'Leg Kicks', 'Damage Asborbtion']
      )
    )
    

    fig = px.line_polar(df, r='r', theta='theta', line_close=True)
    # fig.update_traces(fill='toself')
    # fig.show()
    fig.update_layout(
          polar=dict(
              radialaxis=dict(range=[0, 100], tick0=0, dtick=20)
          )
      )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#EE82EE"),
        xaxis=dict(
            gridcolor="rgba(255,255,255,0.1)",
            zerolinecolor="rgba(255,255,255,0.2)"
        ),
        yaxis=dict(
            gridcolor="rgba(255,255,255,0.1)",
            zerolinecolor="rgba(255,255,255,0.2)"
        )
    )
    
    return fig

def clinching_analysis_plot(fighter_id, db):
    
  stats = db.execute('select clinch_strikes_pm_scaled as clinch_volume, effective_accuracy_scaled as clinch_accuracy, schl as head_hunter, \
            scbl as body_hunter from aggregate_clinching where fighter_id = ?', (fighter_id,)).fetchone()
  
  df = pd.DataFrame(dict(
    r=[stats['clinch_volume'], stats['clinch_accuracy'], stats['head_hunter'], stats['body_hunter']],
    theta=['Clinch Strike Volume', 'Clinch Strike Accuracy', 'Head Hunting', 'Body Hunting']
      )
    )
    

  fig = px.line_polar(df, r='r', theta='theta', line_close=True)
  fig.update_layout(
          polar=dict(
              radialaxis=dict(range=[0, 100], tick0=0, dtick=20)
          )
      )
  fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#EE82EE"),
        xaxis=dict(
            gridcolor="rgba(255,255,255,0.1)",
            zerolinecolor="rgba(255,255,255,0.2)"
        ),
        yaxis=dict(
            gridcolor="rgba(255,255,255,0.1)",
            zerolinecolor="rgba(255,255,255,0.2)"
        )
    )

  return fig

def grappling_analysis_plot(fighter_id, db):
    
  stats = db.execute('''select bjj_defence_scaled as bjj_def, effective_takedowns_scaled as takedowns, td_def_scaled as td_def, 
                    effective_control_scaled as control,  effective_gnp_scaled as gnp, 
                    effective_sub_threat_scaled as sub_threat from aggregate_grappling where fighter_id = ?''', (fighter_id,)).fetchone()
  
  df = pd.DataFrame(dict(
    r=[stats['takedowns'], stats['td_def'], stats['control'], stats['gnp'], stats['sub_threat'], stats['bjj_def']],
    theta=['Takedowns', 'Takedown Defence', 'Ground Control', 'Ground and Pound', 'Sub Threat', 'Sub Defence']
      )
    )
    

  fig = px.line_polar(df, r='r', theta='theta', line_close=True)
  fig.update_layout(
          polar=dict(
              radialaxis=dict(range=[0, 100], tick0=0, dtick=20)
          )
      )
  fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#EE82EE"),
        xaxis=dict(
            gridcolor="rgba(255,255,255,0.1)",
            zerolinecolor="rgba(255,255,255,0.2)"
        ),
        yaxis=dict(
            gridcolor="rgba(255,255,255,0.1)",
            zerolinecolor="rgba(255,255,255,0.2)"
        )
    )

  return fig

def strike_heatmap(fighter_id, db, normalize=True):

    stats = db.execute(
        f"""
        SELECT s.sdhl, s.sdbl, s.sdll, s.sdh_acc, s.sdb_acc, s.sdl_acc, f.name
        FROM aggregate_striking s
        JOIN fighters f
        ON s.fighter_id = f.fighter_id
        WHERE s.fighter_id = ?
        """, (fighter_id,)
    ).fetchone()

    if stats is None:
        return None

    head = stats['sdhl'] or 0
    body = stats['sdbl'] or 0
    legs = stats['sdll'] or 0

    if normalize:
        total = head + body + legs
        if total > 0:
            head /= total
            body /= total
            legs /= total

    heat_data = np.array([
        [legs, legs, legs],
        [body, body, body],
        [head, head, head]
    ])

    y_labels = [
        f"Legs ({stats['sdll']} total, {stats['sdl_acc']*100:.1f}% acc)",
        f"Body ({stats['sdbl']} total, {stats['sdb_acc']*100:.1f}% acc)",
        f"Head ({stats['sdhl']} total, {stats['sdh_acc']*100:.1f}% acc)"
    ]


    fig = go.Figure(data=go.Heatmap(
        z=heat_data,
        y=y_labels,
        x=["", "", ""],
        colorscale="Reds",
        showscale=True,
        colorbar=dict(title="Strike Intensity")
    ))

    fig.update_layout(
        title=f"Strike Targeting Heatmap",
        xaxis_showgrid=False,
        yaxis_showgrid=False,
        xaxis_visible=False,
        template="plotly_white",
        height=400,
        width=400
    )

    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#EE82EE"),
        xaxis=dict(
            gridcolor="rgba(255,255,255,0.1)",
            zerolinecolor="rgba(255,255,255,0.2)"
        ),
        yaxis=dict(
            gridcolor="rgba(255,255,255,0.1)",
            zerolinecolor="rgba(255,255,255,0.2)"
        )
    )

    return fig

def career_plot(fighter_id, db):
    
    stats = db.execute('''select g.wrestling, g.bjj, g.striking, g.gnp, c.career_score, g.global_rating_scaled, g.global_rating, s.true_ko_power_scaled as ko_power from aggregate_global g join aggregate_career c on g.fighter_id = c.fighter_id join aggregate_striking s on g.fighter_id = s.fighter_id where g.fighter_id = ?''', (fighter_id,)).fetchone()
  
    df = pd.DataFrame(dict(
    r=[round(stats['wrestling']), round(stats['bjj']), round(stats['ko_power']), round(stats['striking']), round(stats['gnp']), round(stats['career_score'])],
    theta=['Wrestling', 'BJJ', 'X Factor Power', 'Striking', 'GNP', 'Career']
      )
    )
    

    fig = px.line_polar(df, r='r', theta='theta', line_close=True)
    fig.update_layout(
          polar=dict(
              radialaxis=dict(range=[0, 100], tick0=0, dtick=20)
          )
      )
  
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#EE82EE"),
        xaxis=dict(
            gridcolor="rgba(255,255,255,0.1)",
            zerolinecolor="rgba(255,255,255,0.2)"
        ),
        yaxis=dict(
            gridcolor="rgba(255,255,255,0.1)",
            zerolinecolor="rgba(255,255,255,0.2)"
        )
    )
  
    return fig

def comparison_plot(fighter1, fighter2, db, compare_type="striking"):
    
    import plotly.graph_objects as go

    categories_dict = {
        'striking': ['ss_acc', 'true_ko_power', 'tsl_pm', 'leg_kicks', 'str_def'],
        'grappling': ['td_def', 'tk_acc', 'td_pm', 'control_pm', 'subs_pm', 'sub_attempts_faced_pm', 'gnp_pm'],
    }

    fighters = {}
    list_of_features = []
    query = None

    if compare_type == "career":
        fighters['fighter1_weak'] = get_scaled_attributes(best=False, db=db, fighter_id=fighter1, quantity=3)
        fighters['fighter2_weak'] = get_scaled_attributes(best=False, db=db, fighter_id=fighter2, quantity=3)

        fighters['fighter1_strength'] = get_scaled_attributes(best=True, db=db, fighter_id=fighter1, quantity=3)
        fighters['fighter2_strength'] = get_scaled_attributes(best=True, db=db, fighter_id=fighter2, quantity=3)

        for _, val in fighters.items():
            for key, _ in val.items():
                list_of_features.append(key.replace('_scaled', ''))

        list_of_features = list(set(list_of_features))
        elements = ', '.join(list_of_features)

        query = (
            'select ' + elements + ', opp_avg_elo '
            'from aggregate_striking s '
            'join aggregate_grappling g on s.fighter_id = g.fighter_id '
            'join aggregate_clinching c on s.fighter_id = c.fighter_id '
            'where s.fighter_id = ?'
        )


    if compare_type not in ['striking', 'grappling', 'career']:
        return None


    if not query:
        list_of_features = categories_dict[compare_type]
        elements = ', '.join(list_of_features)
        query = f'select {elements} from aggregate_{compare_type} where fighter_id = ?'

    f1 = db.execute(query, (fighter1,)).fetchone()
    f2 = db.execute(query, (fighter2,)).fetchone()

    if not (f1 and f2):
        return None

    fighter1_values = list(dict(f1).values())
    fighter2_values = list(dict(f2).values())

    # making the visual scaling independant 
    scaled_f1 = []
    scaled_f2 = []

    for v1, v2 in zip(fighter1_values, fighter2_values):
        max_val = max(abs(v1), abs(v2))

        if max_val == 0:
            scaled_f1.append(0)
            scaled_f2.append(0)
        else:
            scaled_f1.append(v1 / max_val)
            scaled_f2.append(v2 / max_val)

    fighter1_plot = [-v for v in scaled_f1]
    fighter2_plot = scaled_f2

    # making the stats human readable
    readable_stats = []

    for stat in list_of_features:
        label = stat

        label = label.replace("ss", "significant strike")
        label = label.replace("tsl", "total strikes landed")
        label = label.replace("_pm", " / minute")
        label = label.replace("_", " ")
        label = label.replace("opp", 'opponent')
        

        label = label.title()

        readable_stats.append(label)


    fig = go.Figure()

    fig.add_trace(go.Bar(
        y=readable_stats,
        x=fighter1_plot,
        name="Fighter 1",
        orientation="h",
        marker_color="blue",
        text=[round(v, 3) for v in fighter1_values],
        textposition="inside"
    ))

    fig.add_trace(go.Bar(
        y=readable_stats,
        x=fighter2_plot,
        name="Fighter 2",
        orientation="h",
        marker_color="purple",
        text=[round(v, 3) for v in fighter2_values],
        textposition="inside"
    ))

    fig.update_layout(
        barmode="relative",
        title="Fighter Comparison",
        xaxis=dict(range=[-1, 1])
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#EE82EE"),
        xaxis=dict(
            gridcolor="rgba(255,255,255,0.1)",
            zerolinecolor="rgba(255,255,255,0.2)"
        ),
        yaxis=dict(
            gridcolor="rgba(255,255,255,0.1)",
            zerolinecolor="rgba(255,255,255,0.2)"
        )
    )

    fig.update_yaxes(autorange="reversed")

    return fig, fighters



# conn = sq.connect(db_path)
# conn.row_factory = sq.Row
# db = conn.cursor()
# # strike_heatmap(2373, db)

# career_plot(2373, db)



