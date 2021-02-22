import pandas as pd
from flask import Flask, send_from_directory
import dash
from dash.dependencies import Input, Output, State
import dash_html_components as html
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import sqlalchemy
import cx_Oracle
import dash_cytoscape as cyto
import pyarrow.parquet as pq
import dash_table

cx_Oracle.init_oracle_client(lib_dir=r"C:\Oracle\instantclient_19_6")


def get_metas():
    #Entrer l'adresse, l'id et le mot de passe de connexion à la BDD
    engine = sqlalchemy.create_engine("",
                                      max_identifier_length=128)
    connection = engine.connect()
    # Requête SQL de récupération des métadonnées :
    query = """
        SELECT
        ut.TABLE_NAME table_name,
        CONS_R.TABLE_NAME table_mere,
        TF_COL.column_name TFCN,
        TM_COL.column_name TMCN
        FROM user_tables ut
        LEFT JOIN (SELECT * FROM USER_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'R') CONS ON CONS.table_name = ut.table_name
        LEFT JOIN USER_CONS_COLUMNS COLS ON COLS.CONSTRAINT_NAME = CONS.CONSTRAINT_NAME
        LEFT JOIN USER_CONSTRAINTS CONS_R ON CONS_R.CONSTRAINT_NAME = CONS.R_CONSTRAINT_NAME
        LEFT JOIN USER_CONS_COLUMNS TM_COL ON TM_COL.CONSTRAINT_NAME = CONS.R_CONSTRAINT_NAME
        LEFT JOIN USER_CONS_COLUMNS TF_COL ON TF_COL.CONSTRAINT_NAME = CONS.CONSTRAINT_NAME
        ORDER BY ut.table_name
        """
    return pd.read_sql_query(query, connection)


def get_columns():
    engine = sqlalchemy.create_engine("oracle+cx_oracle://stagbi25:Phoenix#Icar67@51.91.76.248:15440/coursdb",
                                      max_identifier_length=128)
    connection = engine.connect()
    query = """
    SELECT TABLE_NAME, COLUMN_NAME
    FROM USER_TAB_COLUMNS
    """
    columns_dict = {}
    df_columns = pd.read_sql_query(query, connection)
    for t in df_columns['table_name'].unique():
        columns_dict[t] = df_columns[df_columns['table_name'] == t]['column_name'].tolist()
    return columns_dict


def get_els():
    df_meta = get_metas()
    nodes = [{'type': 'node', 'data': {'id': n, 'label': n}} for n in df_meta['table_name'].unique()]
    edges = [
        {'type': 'edge', 'data': {'source': l[1][1], 'target': l[1][0], 'source_key': l[1][3], 'target_key': l[1][2]}}
        for l in df_meta.iterrows() if l[1][1] and (l[1][0] != l[1][1])]
    return nodes + edges


# Générateur de requêtes sql
class SqlRequest:
    def __init__(self, edges, selected_columns, type='select'):
        print(selected_columns)
        """
        :param edges: liste de dictionnaires de dictionnaires avec la structure suivante:
        [{'first_node':{'name': '...', 'key', '...'}, 'second_node': {'name': '...', 'key', '...'}}, ...]
        Chaque dictionnaire au sein de la liste correspond à une jointure.
        first_node : premier node sélectionné par l'utilisateur
        second_node : second node sélectionné par l'utilisateur
        :param type:
        """
        self.edges = edges
        self.selected_columns = selected_columns
        self.tables_names = set(
            [n['first_node']['name'] for n in self.edges] + [n['second_node']['name'] for n in self.edges])
        self.tables_alias = {t: t[0] + t[1] + t[-3] + t[-2] for t in self.tables_names}
        self.columns_alias = {}
        self.generate_columns_alias()
        print(self.columns_alias)

    def generate_columns_alias(self):
        tables_set = set()
        for table in self.selected_columns:
            self.columns_alias[table] = {}
            for c in self.selected_columns[table]:
                if c not in tables_set:
                    self.columns_alias[table][c] = ""
                    tables_set.add(c)
                else:
                    self.columns_alias[table][c] = f"{table}_{c}"


    def one_join(self, e):
        """
        Méthode qui génère une jointure
        :param e: dictionnaire de données relatives à une jointure
        structure : {'first_node':{'name': '...', 'key', '...'}, 'second_node': {'name': '...', 'key', '...'}}
        :return: string
        """
        return f"INNER JOIN {e['second_node']['name']} {self.tables_alias[e['second_node']['name']]} ON {self.tables_alias[e['second_node']['name']]}.{e['second_node']['key']} = {self.tables_alias[e['first_node']['name']]}.{e['first_node']['key']}"

    def all_join(self, n=0):
        """
        Fonction récursive de construction des jointures
        :param n: compteur de récurrences
        :return: string
        """
        e = self.edges[n]
        if n == len(self.edges) - 1:
            return self.one_join(e)
        else:
            return self.one_join(e) + "\n" + self.all_join(n=n + 1)

    def one_table_columns(self, table, cols, n=0):
        if self.columns_alias[table][cols[n]]: col_name = f"{cols[n]} AS {self.columns_alias[table][cols[n]]}"
        else: col_name = cols[n]

        if n == len(cols) - 1:
            return f"{self.tables_alias[table]}.{col_name}"
        else:
            return f"{self.tables_alias[table]}.{col_name}, " + self.one_table_columns(table, cols, n=n + 1)

    def tables_columns(self):
        st = ""
        for t in self.selected_columns:
            st = st + self.one_table_columns(t, self.selected_columns[t]) + ", "
        return st[:-2]

    def generate(self):
        """
        Génératien de la requête SQL
        :return: string
        """
        request = f"SELECT {self.tables_columns()} FROM {self.edges[0]['first_node']['name']} {self.tables_alias[self.edges[0]['first_node']['name']]}\n"
        request += self.all_join()
        return request


class mySuperDash():
    def __init__(self):
        self.colors = {'selected_color': '#B10DC9',  # Couleur des nodes sélectionnés par l'utilisateur
                       'selectable_color': '#0074D9'}  # Couleur des nodes sélectionnables par l'utilisateur
        self.server = Flask(__name__)
        self.app = dash.Dash(server=self.server, external_stylesheets=[dbc.themes.SUPERHERO])
        self.dl_directory = "download/"
        self.app.css.append_css({'external_url': '/assets/reset.css'})
        self.els = get_els()
        self.user_choices = {'nodes': [],
                             'edges': [],
                             'selectable_nodes': [],
                             'selectable_edges': [],
                             'selected': [],
                             'request_data': [],
                             'tables_columns': get_columns()}
        self.default_stylesheet = [
            {
                "selector": 'node',
                'style': {
                    "opacity": 1,
                    'background-color': self.colors['selectable_color'],
                    'content': 'data(label)',
                    'color': 'white',

                    'font-size': 12,
                    # 'background-color': 'grey',
                    'text-valign': 'bottom',
                    'text-halign': 'center',
                    'background-fit': 'contain',
                    'background-clip': 'none',
                    'text-background-color': '#00001a',
                    'text-background-opacity': 0.7,
                    'text-background-padding': 2,
                    'text-background-shape': 'roundrectangle',
                    'min-zoomed-font-size': 8
                }
            },
            {
                "selector": 'edge',
                'style': {
                    'line-color': 'grey',
                    "curve-style": "bezier",
                    "opacity": 0.70,
                }
            },

        ]
        self.styles = {
            'pre': {
                'border': 'thin lightgrey solid',
                'overflowX': 'scroll'
            },
            'json-output': {
                'overflow-y': 'scroll',
                'height': 'calc(60% - 25px)',
                'border': 'thin lightgrey solid'
            },
            'tab': {'height': 'calc(98vh - 115px)',
                    'width': 'auto'
                    },
        }

    def init_layout(self):
        self.app.layout = dbc.Container(
            [
                html.H1(
                    children='Extract Transform Load Project',

                    className='title'
                ),

                html.Div(className='dropdown_reinit', children=[
                dcc.Dropdown(
                    id='dropdown-update-layout',
                    value='circle',
                    clearable=False,
                    options=[
                        {'label': name.capitalize(), 'value': name}
                        for name in ['grid', 'random', 'circle', 'cose', 'concentric']
                    ],

                ),
                dbc.Button("Réinitialiser", id="reinitialize_button", color="primary"),
                    ]),
                dbc.Row(
                    [dbc.Col(md=1),

                     dbc.Col(cyto.Cytoscape(
                         id='cytoscape',
                         style={'width': '100%', 'height': '95vh', 'overflow': 'unset'},
                         elements=self.els,
                         layout={'name': 'circle', 'radius': 20},
                         stylesheet=self.default_stylesheet,
                     ),
                         md=10),
                     # dbc.Col([dbc.Button("Colonnes", id="columns_button", color="primary"),
                     #          dbc.Button("Générer", id="generate_button", color="primary", className="mt-2"),
                     #          dbc.Button("Tester", id="afficher_tableau", color="primary", className="ml-2")], md=1),
                     ],
                    align="center",
                ),
                # dbc.Row(id='columns_row'),
                # dbc.Row(
                #     [
                #         dbc.Col(dbc.Textarea(), id="affichage_test", md=12)
                #     ]
                # ),
                # dbc.Row(id='test_button'),
                #
                # dbc.Row(id="table_df"),
                html.Div(className='twelve columns', children=[

                    dcc.Tabs(id='tabs', children=[
                        dcc.Tab(label='Hover Data', id="tabs_id", children=[
                            html.Div(style=self.styles['tab'], children=[
                                html.P('Liste des champs:'),
                                html.Pre(
                                    id='mouseover-node-data-json-output',
                                    style=self.styles['json-output']
                                ),
                                html.P('Relation mère - fille:'),
                                html.Pre(
                                    id='mouseover-edge-data-json-output',
                                    style=self.styles['json-output']
                                )
                            ])
                        ]),
                        dcc.Tab(label='Choix des colonnes', value='tab_colonnes', children=[
                            html.Div(style=self.styles['tab'], children=[
                                html.P('Selection des tables :'),
                                dbc.Col([
                                        dbc.Button("Ajouter les Colonnes", id="columns_button", color="primary"),
                                         ], md=4),


                                dbc.Row(id='columns_row', style=self.styles['json-output']),
                            ])
                        ]),

                        dcc.Tab(label='Requète', children=[
                            html.Div(style=self.styles['tab'], children=[

                                dbc.Row(id='test_button'),

                                # dbc.Row(id="table_df"),



                                html.P('Générateur de requête:'),
                                dbc.Row([dbc.Col(dbc.Button("Générer", id="generate_button", color="primary", className="btn_data"), md=2),
                                        dbc.Col(id="btn_dl_sql", className="btn_data", md=2)]),
                                html.Pre(
                                    id='affichage_test',
                                    style=self.styles['json-output']
                                ),

                                html.P('Afficher le résultat de la requète:'),
                                dbc.Row( id="row_btns_data", className="mb-1"),
                                html.Pre(
                                    id='table_df',
                                    style=self.styles['json-output']
                                ),

                            ])
                        ]),
                    ]),
                ]),
            ]
        )

    def initCallbacks(self):
        @self.app.callback(Output('cytoscape', 'stylesheet'),
                           Input('cytoscape', 'tapNode'),
                           Input('reinitialize_button', 'n_clicks'),
                           State('cytoscape', 'stylesheet'))
        def new_stylesheet(node, n_clicks, st):
            """
            Fonction de callback de gestion des choix utilisateur et de genération de la stylesheet
            :param node: node sélectionné par l'utilisateur
            :param st: stylesheet précédente
            :return: stylesheet
            """
            ctx = dash.callback_context
            if ctx.triggered:
                print(ctx.triggered)
                if ctx.triggered[0]['prop_id']=='reinitialize_button.n_clicks':
                    self.user_choices = {'nodes': [],
                                         'edges': [],
                                         'selectable_nodes': [],
                                         'selectable_edges': [],
                                         'selected': [],
                                         'request_data': [],
                                         'tables_columns': get_columns()}
                    return self.default_stylesheet


            reselect = False  # Sera passé à True si le node a déjà été selectionné

            if not node: return st

            # Si l'utilisateur sélectionne un node non sélectionnable l'ancienne stylesheet est retournée:
            if node['data']['id'] not in self.user_choices['selectable_nodes'] and self.user_choices['nodes']: return st

            if node['data']['id'] in self.user_choices['nodes']: reselect = True

            if not self.user_choices['nodes']: self.user_choices['selectable_nodes'].append(node['data']['id'])

            if not reselect:
                for e in node['edgesData']:  # boucle sur l'ensemble des edges liés au node sélectionné
                    if self.user_choices['nodes']:
                        if self.user_choices['selected'][-1] in [e['source'], e['target']]:
                            self.user_choices['edges'].append(e['id'])
                            # Création du dictionnaire de données pour la future jointure:
                            data_request = {'first_node': {}, 'second_node': {}}
                            if node['data']['id'] == e['source']:
                                data_request['first_node']['name'], data_request['second_node']['name'] = e['target'], \
                                                                                                          e['source']
                                data_request['first_node']['key'], data_request['second_node']['key'] = e['target_key'], \
                                                                                                        e['source_key']
                            elif node['data']['id'] == e['target']:
                                data_request['first_node']['name'], data_request['second_node']['name'] = e['source'], \
                                                                                                          e['target']
                                data_request['second_node']['key'], data_request['first_node']['key'] = e['target_key'], \
                                                                                                        e['source_key']
                            self.user_choices['request_data'].append(data_request)
                    self.user_choices['selectable_edges'].append(e['id'])
                    if node['data']['id'] == e['target']:
                        if e['source'] not in self.user_choices['selectable_nodes']: self.user_choices[
                            'selectable_nodes'].append(e['source'])
                    else:
                        if e['target'] not in self.user_choices['selectable_nodes']: self.user_choices[
                            'selectable_nodes'].append(e['target'])

            nst = [{
                "selector": 'node',
                'style': {
                    'content': 'data(label)',
                    'color': 'white',
                    'font-family': 'Fira Mono',
                    'font-size': 12,
                    'background-color': 'grey',
                    'text-valign': 'bottom',
                    'text-halign': 'center',
                    'background-fit': 'contain',
                    'background-clip': 'none',
                    'text-background-color': '#00001a',
                    'text-background-opacity': 0.7,
                    'text-background-padding': 2,
                    'text-background-shape': 'roundrectangle',
                    'min-zoomed-font-size': 8
                    }
                },
                {
                    'selector': 'edge',
                    'style': {
                        "line-color": 'grey',
                        'opacity': 1,
                        "curve-style": "bezier",
                    }
                }]

            nst.append({
                "selector": 'node[id = "{}"]'.format(node['data']['id']),
                "style": {
                    'background-color': self.colors['selected_color'],
                    "border-color": "black",
                    "border-width": 2,
                    "border-opacity": 1,
                    "opacity": 1,
                    "label": "data(label)",
                    "color": "#B10DC9",
                    "text-opacity": 1,
                    "font-size": 12,
                }
            })
            for n in self.user_choices['nodes']:
                if not (n == node['data']['id'] and reselect):
                    nst.append({
                        "selector": 'node[id = "{}"]'.format(n),
                        "style": {
                            'background-color': self.colors['selected_color'],
                            "border-color": "purple",
                            "border-width": 2,
                            "border-opacity": 1,
                            "opacity": 1,
                            "label": "data(label)",
                            "color": "#B10DC9",
                            "text-opacity": 1,
                            "font-size": 12,
                        }
                    })
            for n in self.user_choices['selectable_nodes']:
                if n not in self.user_choices['nodes'] and n != node['data']['id']:
                    nst.append({
                        "selector": 'node[id = "{}"]'.format(n),
                        "style": {
                            'background-color': self.colors['selectable_color'],
                            'opacity': 1,
                            "text-opacity": 1,
                        }
                    })
            for e in self.user_choices['edges']:
                nst.append({
                    "selector": 'edge[id= "{}"]'.format(e),
                    "style": {
                        "mid-target-arrow-color": 'orange',
                        "mid-target-arrow-shape": "vee",
                        "line-color": self.colors['selected_color'],
                        'opacity': 0.8,
                        "text-opacity": 1
                    }
                })
            for e in self.user_choices['selectable_edges']:
                if e not in self.user_choices['edges']:
                    nst.append({
                        "selector": 'edge[id= "{}"]'.format(e),
                        "style": {
                            "mid-target-arrow-color": 'orange',
                            "mid-target-arrow-shape": "vee",
                            "line-color": self.colors['selectable_color'],
                            'opacity': 0.8,
                            "text-opacity": 1
                        }
                    })
            nst.append({'selector': '.dcc_control',
                        'style': {
                            'margin': '0',
                            'padding': '5px',
                            'width': 'calc(100%-40px)'
                        }})

            self.user_choices['selected'].append(node['data']['id'])
            if not reselect: self.user_choices['nodes'].append(node['data']['id'])

            return nst

        @self.server.route("/download/<path:path>")
        def download(path):
            return send_from_directory(self.dl_directory, path, as_attachment=True)


        @self.app.callback(Output('columns_row', 'children'),
                           Input('columns_button', 'n_clicks'),
                           State('columns_row', 'children'))
        def show_columns(n_clicks, rw):
            ma_liste2 = []
            for t in self.user_choices['nodes']:
                cols_list = [c for c in self.user_choices['tables_columns'][t]]
                cols_list2 = [
                    {"label": c, "value": c}
                    for c in self.user_choices['tables_columns'][t]
                ]
                ma_liste2.append(dbc.Col([dbc.Button(t, color='primary', className="ml-1 align-middle"),
                                          dcc.Dropdown(
                                              options=cols_list2,
                                              multi=True,
                                              value=cols_list,
                                              className="dcc_control",
                                          )], md=3, className='align-middle'))
            return ma_liste2

        @self.app.callback(Output('affichage_test', 'children'),
                           Output('btn_dl_sql', 'children'),
                           Output('row_btns_data', 'children'),
                           Input('generate_button', 'n_clicks'),
                           State('columns_row', 'children'))
        def sql_request(n_clicks, div):
            if self.user_choices['request_data']:
                cols_dict = {}
                for card in div:
                    # print(card['props']['children'])
                    # print(card['props']['children'][1]['props']['value'])
                    # print(card['props']['children'][0]['props']['children'])
                    cols_dict[card['props']['children'][0]['props']['children']] = \
                    card['props']['children'][1]['props']['value']

                req = SqlRequest(self.user_choices['request_data'], cols_dict)
                req_sql = req.generate()
                print(req_sql)
                with open("download/req.sql", "w") as f:
                    f.write(req_sql)

                btns_sql = html.A(dbc.Button("Télécharger", id="dl_sql_button", color="primary", className="btn_data"), download='req.sql', href="/download/req.sql", target="_blank")
                btns_data = [dbc.Col(dbc.Button("Tester", id="afficher_tableau", className="btn_data", color="primary"), id='test_dl_col', md=2),
                                        dbc.Col(dbc.Button("Exécuter", id="execute_button", className="btn_data", color="primary"), id="col_executer", md=2),
                                        dbc.Col(id="col_telecharger", md=2)]
                return req_sql, btns_sql, btns_data
            else: return ['', '', '']

        """
        @self.app.callback(Output('test_button', 'children'),
                           Input('affichage_test', 'n_clicks'))
        def sql_request(n_clicks):
            if self.user_choices['nodes']:
                return dbc.Col(, md=1)
        """

        @self.app.callback(Output('table_df', 'children'),
                           Input('afficher_tableau', 'n_clicks'),
                           State('affichage_test', 'children'))
        def show_response(n_clicks, query):
            if self.user_choices['nodes'] and n_clicks:
                query = query + " FETCH FIRST 5 ROWS ONLY"
                engine = sqlalchemy.create_engine("oracle+cx_oracle://stagbi25:Phoenix#Icar67@51.91.76.248:15440/coursdb",
                                                  max_identifier_length=128)
                connection = engine.connect()
                df_test = pd.read_sql_query(query, connection)
                print(df_test)
                table = dbc.Table.from_dataframe(df_test.head(5), striped=True, bordered=True, hover=True, dark=True)

                return table


        @self.app.callback(Output('col_telecharger', 'children'),
                           Input('execute_button', 'n_clicks'),
                           State('affichage_test', 'children'))
        def execute_request(n_clicks, query):
            if self.user_choices['nodes'] and n_clicks:
                query = query + " FETCH FIRST 50 ROWS ONLY"
                engine = sqlalchemy.create_engine("oracle+cx_oracle://stagbi25:Phoenix#Icar67@51.91.76.248:15440/coursdb",
                                                  max_identifier_length=128)
                connection = engine.connect()
                df_data = pd.read_sql_query(query, connection)
                df_data.to_parquet('download/data.parquet')
                dl_button = html.A(dbc.Button("Télécharger", id="dl_button", color="primary", className="btn_data"), download='data.parquet', href="/download/data.parquet", target="_blank")
                return dl_button


        @self.app.callback(Output('cytoscape', 'layout'),
                      [Input('dropdown-update-layout', 'value')])
        def update_layout(layout):
            return {
                'name': layout,
                'animate': True
            }

        @self.app.callback(Output('mouseover-node-data-json-output', 'children'),
                      [Input('cytoscape', 'mouseoverNodeData')])
        def displayMouseoverNodeData(data_list):
            if not data_list:
                return
            engine = sqlalchemy.create_engine("oracle+cx_oracle://stagbi25:Phoenix#Icar67@51.91.76.248:15440/coursdb",
                                              max_identifier_length=128)
            connection = engine.connect()
            hoover = []

            sql = '''
                select  cols.column_name, col_cons.constraint_name, data_type, usr_cons.constraint_type FROM user_tab_columns cols
                left join user_cons_columns col_cons 
                on col_cons.table_name = cols.table_name
                and col_cons.column_name = cols.column_name
                left join user_constraints usr_cons
                on  usr_cons.table_name = cols.table_name
                and usr_cons.constraint_name = col_cons.constraint_name WHERE cols.table_name = :tb
                ORDER BY constraint_name
            '''
            var = data_list['id']

            content = pd.read_sql_query(sql, connection, params={'tb': ''.join(var)})
            content = content.rename(columns={'column_name': 'liste champs', 'constraint_name': 'nom de la contrainte',
                                              'data_type': 'type de donnée', 'constraint_type': 'type de la contrainte'})

            hoover.append(dbc.Col([dbc.Button(var, color='primary', className="ml-1 align-middle"),
                                   dash_table.DataTable(
                                       id='table',
                                       columns=[{"name": i, "id": i} for i in content.columns],
                                       data=content.to_dict('records'))
                                     ], className="hover-data"))

            return hoover


    def run_server(self, *args, **kwargs):
        self.app.run_server(*args, **kwargs)


if __name__ == '__main__':
    app = mySuperDash()
    app.init_layout()
    app.initCallbacks()

    app.run_server(debug=False)
